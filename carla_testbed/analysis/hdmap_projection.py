from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.apollo_hdmap_projection import (
    analyze_apollo_hdmap_projection_file,
    apollo_hdmap_projection_summary_md,
    read_apollo_hdmap_projection,
    summarize_apollo_hdmap_projection,
    write_apollo_hdmap_projection_report,
)

HDMAP_PROJECTION_SCHEMA_VERSION = "hdmap_projection.v1"


def analyze_hdmap_projection_file(path: str | Path | None) -> dict[str, Any]:
    report = analyze_apollo_hdmap_projection_file(path)
    report["schema_version"] = HDMAP_PROJECTION_SCHEMA_VERSION
    report["compatibility_report"] = "apollo_hdmap_projection_report.v1"
    return report


def analyze_hdmap_projection_rows(rows: Sequence[Mapping[str, Any]] | None) -> dict[str, Any]:
    summary = summarize_apollo_hdmap_projection(rows)
    return {
        "schema_version": HDMAP_PROJECTION_SCHEMA_VERSION,
        "compatibility_report": "apollo_hdmap_projection_report.v1",
        "status": summary.get("status"),
        "claim_grade": summary.get("claim_grade"),
        "projection": summary,
        "blocking_reasons": list(summary.get("blocking_reasons") or []),
        "warnings": list(summary.get("warnings") or []),
        "missing_fields": list(summary.get("missing_fields") or []),
        "suspected_failure_layers": list(summary.get("suspected_failure_layers") or []),
        "interpretation_boundary": (
            "This HDMap projection wrapper is a canonical analysis surface for "
            "Apollo HDMap API projection evidence. Missing official projection "
            "artifact remains insufficient_data and cannot verify reference-line claims."
        ),
    }


__all__ = [
    "HDMAP_PROJECTION_SCHEMA_VERSION",
    "analyze_hdmap_projection_file",
    "analyze_hdmap_projection_rows",
    "apollo_hdmap_projection_summary_md",
    "read_apollo_hdmap_projection",
    "write_apollo_hdmap_projection_report",
]
