from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

OFFICIAL_SOURCE = "apollo_hdmap_api"
HDMAP_PROJECTION_REPORT_SCHEMA_VERSION = "apollo_hdmap_projection_report.v1"

HDMAP_HEADING_WARN_RAD = 0.03
HDMAP_HEADING_FAIL_RAD = 0.05
HDMAP_LATERAL_WARN_M = 0.30
HDMAP_LATERAL_FAIL_M = 0.50
HDMAP_MIN_CLAIM_GRADE_ROWS = 50
HDMAP_MIN_OK_RATIO = 0.95
HDMAP_MIN_PROJECTION_COVERAGE_M = 30.0
HDMAP_MIN_SIM_TIME_COVERAGE_RATIO = 0.80
HDMAP_MIN_ROUTE_S_COVERAGE_RATIO = 0.50


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


def analyze_apollo_hdmap_projection_file(path: str | Path | None) -> dict[str, Any]:
    artifact_path = Path(path).expanduser() if path else None
    artifact_file_exists = bool(artifact_path and artifact_path.exists())
    rows = read_apollo_hdmap_projection(artifact_path)
    summary = summarize_apollo_hdmap_projection(rows, artifact_file_exists=artifact_file_exists)
    return {
        "schema_version": HDMAP_PROJECTION_REPORT_SCHEMA_VERSION,
        "status": summary["status"],
        "claim_grade": summary["claim_grade"],
        "artifact_status": summary["artifact_status"],
        "artifact_path": str(artifact_path) if artifact_path else None,
        "artifact_file_exists": artifact_file_exists,
        "projection": summary,
        "blocking_reasons": list(summary.get("blocking_reasons") or []),
        "warnings": list(summary.get("warnings") or []),
        "missing_fields": list(summary.get("missing_fields") or []),
        "suspected_failure_layers": list(summary.get("suspected_failure_layers") or []),
        "interpretation_boundary": (
            "This report only verifies Apollo HDMap API projection evidence. "
            "It does not prove Planning, Control, perception, or closed-loop behavior."
        ),
    }


def write_apollo_hdmap_projection_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "apollo_hdmap_projection_report.json"
    summary_path = output_dir / "apollo_hdmap_projection_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(apollo_hdmap_projection_summary_md(report), encoding="utf-8")
    return {
        "apollo_hdmap_projection_report": str(json_path),
        "apollo_hdmap_projection_summary": str(summary_path),
    }


def apollo_hdmap_projection_summary_md(report: Mapping[str, Any]) -> str:
    projection = report.get("projection") if isinstance(report.get("projection"), Mapping) else {}
    return "\n".join(
        [
            "# Apollo HDMap Projection Summary",
            "",
            f"- Status: `{report.get('status')}`",
            f"- Claim-grade: `{report.get('claim_grade')}`",
            f"- Artifact status: `{report.get('artifact_status')}`",
            f"- Artifact: `{report.get('artifact_path')}`",
            f"- Artifact exists: `{report.get('artifact_file_exists')}`",
            f"- Official source available: `{projection.get('official_source_available')}`",
            f"- Row count: `{projection.get('row_count')}`",
            f"- Official row count: `{projection.get('official_row_count')}`",
            f"- OK row count: `{projection.get('ok_row_count')}`",
            f"- OK ratio: `{projection.get('ok_ratio')}`",
            f"- Sim-time coverage ratio: `{projection.get('sim_time_coverage_ratio')}`",
            f"- Projection-s coverage m: `{projection.get('projection_s_coverage_m')}`",
            f"- Route-s coverage ratio: `{projection.get('route_s_coverage_ratio')}`",
            f"- Heading error p95 rad: `{projection.get('heading_error_p95_rad')}`",
            f"- Lateral error p95 m: `{projection.get('lateral_error_p95_m')}`",
            f"- Nearest lane ids: `{', '.join(projection.get('nearest_lane_id_topk') or []) or 'none'}`",
            f"- Warnings: `{', '.join(report.get('warnings') or []) or 'none'}`",
            f"- Blocking reasons: `{', '.join(report.get('blocking_reasons') or []) or 'none'}`",
            "",
            str(report.get("interpretation_boundary") or ""),
            "",
        ]
    )


def summarize_apollo_hdmap_projection(
    rows: Sequence[Mapping[str, Any]] | None,
    *,
    artifact_file_exists: bool | None = None,
) -> dict[str, Any]:
    all_rows = [dict(row) for row in (rows or [])]
    official_rows = [row for row in all_rows if str(row.get("source") or "") == OFFICIAL_SOURCE]
    ok_rows = [row for row in official_rows if str(row.get("status") or "").lower() == "ok"]
    status_counts = Counter(str(row.get("status") or "missing") for row in official_rows)
    heading_p95 = _p95_abs(_num(row.get("heading_error_rad")) for row in ok_rows)
    lateral_p95 = _p95_abs(_num(row.get("lateral_error_m")) for row in ok_rows)
    timestamps = [_num(row.get("timestamp")) for row in ok_rows]
    projection_s_values = [_num(row.get("projection_s")) for row in ok_rows]
    sim_time_coverage_s = _span(timestamps)
    projection_s_coverage_m = _span(projection_s_values)
    expected_duration_s = _first_num(row.get("expected_duration_s") or row.get("run_duration_s") for row in all_rows)
    expected_route_distance_m = _first_num(
        row.get("expected_route_distance_m") or row.get("route_length_m") for row in all_rows
    )
    sim_time_coverage_ratio = (
        sim_time_coverage_s / expected_duration_s
        if sim_time_coverage_s is not None and expected_duration_s and expected_duration_s > 0
        else None
    )
    route_s_coverage_ratio = (
        projection_s_coverage_m / expected_route_distance_m
        if projection_s_coverage_m is not None and expected_route_distance_m and expected_route_distance_m > 0
        else None
    )
    route_s_coverage_threshold_m = (
        min(HDMAP_MIN_PROJECTION_COVERAGE_M, 0.5 * expected_route_distance_m)
        if expected_route_distance_m and expected_route_distance_m > 0
        else HDMAP_MIN_PROJECTION_COVERAGE_M
    )

    warnings: list[str] = []
    blocking: list[str] = []
    suspected_layers: list[str] = []
    missing_fields: list[str] = []

    if not all_rows:
        artifact_present = bool(artifact_file_exists)
        artifact_status = "artifact_empty" if artifact_present else "artifact_missing"
        warning = "apollo_hdmap_projection_empty" if artifact_present else "apollo_hdmap_projection_missing"
        missing = "apollo_hdmap_projection_rows" if artifact_present else "apollo_hdmap_projection"
        return {
            "file_present": artifact_present,
            "artifact_status": artifact_status,
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
            "warnings": [warning],
            "blocking_reasons": [],
            "missing_fields": [missing],
            "suspected_failure_layers": [],
            "interpretation": (
                (
                    "Apollo HDMap API projection artifact exists but contains no rows; reference-line "
                    "verification remains insufficient until the exporter records projection samples."
                    if artifact_present
                    else "No Apollo HDMap API projection artifact was found; reference-line "
                    "verification remains insufficient unless another explicit verified "
                    "artifact is present."
                )
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
    if official_rows and len(ok_rows) < HDMAP_MIN_CLAIM_GRADE_ROWS:
        blocking.append("apollo_hdmap_projection_sample_count_low")
    if ok_ratio is not None and ok_ratio < HDMAP_MIN_OK_RATIO:
        blocking.append("apollo_hdmap_projection_ok_ratio_low")
    if official_rows and projection_s_coverage_m is None:
        missing_fields.append("projection_s")
        blocking.append("apollo_hdmap_projection_route_s_coverage_missing")
    elif projection_s_coverage_m is not None and projection_s_coverage_m < route_s_coverage_threshold_m:
        blocking.append("apollo_hdmap_projection_route_s_coverage_low")
    if sim_time_coverage_ratio is not None and sim_time_coverage_ratio < HDMAP_MIN_SIM_TIME_COVERAGE_RATIO:
        blocking.append("apollo_hdmap_projection_sim_time_coverage_low")
    if route_s_coverage_ratio is not None and route_s_coverage_ratio < HDMAP_MIN_ROUTE_S_COVERAGE_RATIO:
        blocking.append("apollo_hdmap_projection_route_s_coverage_ratio_low")
    map_names = {str(row.get("map_name") or "") for row in official_rows if str(row.get("map_name") or "").strip()}
    map_dirs = {str(row.get("map_dir") or "") for row in official_rows if str(row.get("map_dir") or "").strip()}
    if len(map_names) > 1 or len(map_dirs) > 1:
        blocking.append("apollo_hdmap_projection_map_identity_inconsistent")

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
        "artifact_status": "projection_rows_present",
        "available": bool(official_rows),
        "official_source_available": bool(official_rows),
        "status": status,
        "claim_grade": status == "pass",
        "source": OFFICIAL_SOURCE if official_rows else None,
        "row_count": len(all_rows),
        "official_row_count": len(official_rows),
        "ok_row_count": len(ok_rows),
        "ok_ratio": ok_ratio,
        "min_claim_grade_rows": HDMAP_MIN_CLAIM_GRADE_ROWS,
        "min_ok_ratio": HDMAP_MIN_OK_RATIO,
        "min_projection_s_coverage_m": HDMAP_MIN_PROJECTION_COVERAGE_M,
        "min_sim_time_coverage_ratio": HDMAP_MIN_SIM_TIME_COVERAGE_RATIO,
        "min_route_s_coverage_ratio": HDMAP_MIN_ROUTE_S_COVERAGE_RATIO,
        "sim_time_coverage_s": sim_time_coverage_s,
        "expected_duration_s": expected_duration_s,
        "sim_time_coverage_ratio": sim_time_coverage_ratio,
        "projection_s_coverage_m": projection_s_coverage_m,
        "expected_route_distance_m": expected_route_distance_m,
        "route_s_coverage_ratio": route_s_coverage_ratio,
        "route_s_coverage_threshold_m": route_s_coverage_threshold_m,
        "map_identity_consistent": len(map_names) <= 1 and len(map_dirs) <= 1,
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


def _span(values: Iterable[float | None]) -> float | None:
    finite = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    if len(finite) < 2:
        return None
    return finite[-1] - finite[0]


def _num(value: Any) -> float | None:
    if value in {None, "", "nan", "NaN"}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _first_num(values: Iterable[Any]) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _topk(values: Iterable[str], *, k: int = 5) -> list[str]:
    counter = Counter(value for value in values if str(value).strip())
    return [item for item, _ in counter.most_common(k)]
