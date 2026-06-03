from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

OFFICIAL_SOURCE = "apollo_hdmap_api"

HDMAP_HEADING_WARN_RAD = 0.10
HDMAP_HEADING_FAIL_RAD = 0.20
HDMAP_LATERAL_WARN_M = 1.00
HDMAP_LATERAL_FAIL_M = 3.00


def read_apollo_hdmap_projection(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    file_path = Path(path)
    if not file_path.exists():
        return []
    if file_path.suffix.lower() == ".jsonl":
        return _read_jsonl(file_path)
    payload = _read_json(file_path)
    rows = payload.get("rows") or payload.get("projection_rows") or payload.get("samples") or []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def summarize_apollo_hdmap_projection(rows: Sequence[Mapping[str, Any]] | None) -> dict[str, Any]:
    all_rows = [dict(row) for row in (rows or [])]
    official_rows = [row for row in all_rows if str(row.get("source") or "") == OFFICIAL_SOURCE]
    ok_rows = [row for row in official_rows if str(row.get("status") or "").lower() == "ok"]
    status_counts = Counter(str(row.get("status") or "missing") for row in official_rows)
    heading_p95 = _p95_abs(_num(row.get("heading_error_rad")) for row in ok_rows)
    lateral_p95 = _p95_abs(_num(row.get("lateral_error_m")) for row in ok_rows)

    warnings: list[str] = []
    blocking: list[str] = []
    suspected_layers: list[str] = []
    missing_fields: list[str] = []

    if not all_rows:
        return {
            "file_present": False,
            "available": False,
            "official_source_available": False,
            "status": "insufficient_data",
            "claim_grade": False,
            "source": None,
            "row_count": 0,
            "official_row_count": 0,
            "ok_row_count": 0,
            "ok_ratio": None,
            "status_counts": {},
            "heading_error_p95_rad": None,
            "lateral_error_p95_m": None,
            "nearest_lane_id_topk": [],
            "map_name_topk": [],
            "map_dir_topk": [],
            "warnings": ["apollo_hdmap_projection_missing"],
            "blocking_reasons": [],
            "missing_fields": ["apollo_hdmap_projection"],
            "suspected_failure_layers": [],
            "interpretation": (
                "No Apollo HDMap API projection artifact was found; reference-line "
                "verification remains insufficient unless another explicit verified "
                "artifact is present."
            ),
        }

    if not official_rows:
        warnings.append("apollo_hdmap_projection_source_not_official")
        missing_fields.append("source=apollo_hdmap_api")

    if official_rows and not ok_rows:
        blocking.append("apollo_hdmap_projection_no_ok_rows")
        suspected_layers.extend(["map_alignment", "lane_id", "routing_snap"])

    if official_rows and heading_p95 is None:
        missing_fields.append("heading_error_rad")
    if official_rows and lateral_p95 is None:
        missing_fields.append("lateral_error_m")
    if official_rows and (heading_p95 is None or lateral_p95 is None):
        warnings.append("apollo_hdmap_projection_metrics_missing")

    if heading_p95 is not None:
        if heading_p95 >= HDMAP_HEADING_FAIL_RAD:
            blocking.append("apollo_hdmap_projection_heading_error_high")
            suspected_layers.extend(["map_alignment", "lane_direction", "routing_snap"])
        elif heading_p95 >= HDMAP_HEADING_WARN_RAD:
            warnings.append("apollo_hdmap_projection_heading_error_elevated")
            suspected_layers.extend(["map_alignment", "lane_direction"])

    if lateral_p95 is not None:
        if lateral_p95 >= HDMAP_LATERAL_FAIL_M:
            blocking.append("apollo_hdmap_projection_lateral_error_high")
            suspected_layers.extend(["map_alignment", "lane_id", "routing_snap"])
        elif lateral_p95 >= HDMAP_LATERAL_WARN_M:
            warnings.append("apollo_hdmap_projection_lateral_error_elevated")
            suspected_layers.extend(["map_alignment", "lane_id"])

    non_ok_count = len(official_rows) - len(ok_rows)
    ok_ratio = len(ok_rows) / len(official_rows) if official_rows else None
    if official_rows and non_ok_count:
        warnings.append("apollo_hdmap_projection_non_ok_rows_present")

    if blocking:
        status = "fail"
    elif not official_rows or missing_fields:
        status = "insufficient_data"
    elif warnings:
        status = "warn"
    else:
        status = "pass"

    return {
        "file_present": True,
        "available": bool(official_rows),
        "official_source_available": bool(official_rows),
        "status": status,
        "claim_grade": status == "pass",
        "source": OFFICIAL_SOURCE if official_rows else None,
        "row_count": len(all_rows),
        "official_row_count": len(official_rows),
        "ok_row_count": len(ok_rows),
        "ok_ratio": ok_ratio,
        "status_counts": dict(status_counts),
        "heading_error_p95_rad": heading_p95,
        "lateral_error_p95_m": lateral_p95,
        "nearest_lane_id_topk": _topk(str(row.get("nearest_lane_id") or "") for row in official_rows),
        "map_name_topk": _topk(str(row.get("map_name") or "") for row in official_rows),
        "map_dir_topk": _topk(str(row.get("map_dir") or "") for row in official_rows),
        "warnings": sorted(set(warnings)),
        "blocking_reasons": sorted(set(blocking)),
        "missing_fields": sorted(set(missing_fields)),
        "suspected_failure_layers": sorted(set(suspected_layers)),
        "interpretation": (
            "Official Apollo HDMap API projection evidence is present."
            if official_rows
            else "Projection rows exist, but none declare source=apollo_hdmap_api."
        ),
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _p95_abs(values: Iterable[float | None]) -> float | None:
    finite = sorted(abs(float(value)) for value in values if value is not None and math.isfinite(float(value)))
    if not finite:
        return None
    if len(finite) == 1:
        return finite[0]
    position = (len(finite) - 1) * 0.95
    lower = int(position)
    upper = min(lower + 1, len(finite) - 1)
    fraction = position - lower
    return finite[lower] * (1.0 - fraction) + finite[upper] * fraction


def _num(value: Any) -> float | None:
    if value in {None, "", "nan", "NaN"}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _topk(values: Iterable[str], *, k: int = 5) -> list[str]:
    counter = Counter(value for value in values if str(value).strip())
    return [item for item, _ in counter.most_common(k)]
