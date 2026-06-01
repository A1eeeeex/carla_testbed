from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

SCHEMA_VERSION = "apollo_replay_digest.v1"
REPORT_SCHEMA_VERSION = "replay_comparison_report.v1"
REQUIRED_SECTIONS = ("planning", "control", "localization", "chassis")
REQUIRED_METRICS = {
    "planning": (
        "message_count",
        "hz",
        "trajectory_point_count_mean",
        "trajectory_point_count_p95",
        "trajectory_duration_mean_s",
        "curvature_abs_p95",
        "speed_mean",
        "max_gap_ms",
    ),
    "control": (
        "message_count",
        "hz",
        "throttle_mean",
        "brake_mean",
        "steer_mean",
        "steer_abs_p95",
        "max_gap_ms",
    ),
    "localization": ("message_count", "hz"),
    "chassis": ("message_count", "hz"),
}
DEFAULT_TOLERANCES = {
    "planning.message_count": {"warn_abs": 5.0, "fail_abs": 20.0},
    "planning.hz": {"warn_abs": 1.0, "fail_abs": 2.0, "zero_fail": True},
    "planning.trajectory_point_count_mean": {"warn_rel": 0.1, "fail_rel": 0.25},
    "planning.trajectory_point_count_p95": {"warn_rel": 0.1, "fail_rel": 0.25},
    "planning.trajectory_duration_mean_s": {"warn_abs": 0.2, "fail_abs": 0.5},
    "planning.curvature_abs_p95": {"warn_abs": 0.02, "fail_abs": 0.08},
    "planning.speed_mean": {"warn_abs": 0.5, "fail_abs": 1.5},
    "planning.max_gap_ms": {"warn_abs": 100.0, "fail_abs": 500.0},
    "control.message_count": {"warn_abs": 5.0, "fail_abs": 20.0},
    "control.hz": {"warn_abs": 1.0, "fail_abs": 2.0, "zero_fail": True},
    "control.throttle_mean": {"warn_abs": 0.05, "fail_abs": 0.15},
    "control.brake_mean": {"warn_abs": 0.05, "fail_abs": 0.15},
    "control.steer_mean": {"warn_abs": 0.05, "fail_abs": 0.15},
    "control.steer_abs_p95": {"warn_abs": 0.08, "fail_abs": 0.2},
    "control.max_gap_ms": {"warn_abs": 100.0, "fail_abs": 500.0},
    "localization.message_count": {"warn_abs": 5.0, "fail_abs": 20.0},
    "localization.hz": {"warn_abs": 1.0, "fail_abs": 2.0, "zero_fail": True},
    "chassis.message_count": {"warn_abs": 5.0, "fail_abs": 20.0},
    "chassis.hz": {"warn_abs": 1.0, "fail_abs": 2.0, "zero_fail": True},
}


class ReplayDigestError(ValueError):
    pass


@dataclass(frozen=True)
class ReplayDigestComparison:
    report: dict[str, Any]

    @property
    def status(self) -> str:
        return str(self.report.get("status") or "fail")

    @property
    def ok(self) -> bool:
        return self.status != "fail"


def _status_rank(status: str) -> int:
    return {"pass": 0, "warn": 1, "fail": 2}.get(status, 2)


def _combine_status(current: str, candidate: str) -> str:
    return candidate if _status_rank(candidate) > _status_rank(current) else current


def load_replay_digest(path: str | Path) -> dict[str, Any]:
    digest_path = Path(path).expanduser()
    payload = json.loads(digest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ReplayDigestError(f"replay digest must be a mapping: {digest_path}")
    errors = validate_replay_digest(payload)
    if errors:
        raise ReplayDigestError("; ".join(errors))
    payload["_source_path"] = str(digest_path)
    return payload


def validate_replay_digest(digest: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if digest.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if not digest.get("source_record"):
        errors.append("source_record is required")
    if not digest.get("apollo_variant_id"):
        errors.append("apollo_variant_id is required")
    for section in REQUIRED_SECTIONS:
        value = digest.get(section)
        if not isinstance(value, Mapping):
            errors.append(f"{section} must be a mapping")
            continue
        for metric in REQUIRED_METRICS[section]:
            if metric not in value:
                errors.append(f"{section}.{metric} is required")
    return errors


def _metric_value(digest: Mapping[str, Any], dotted_metric: str) -> Any:
    section, metric = dotted_metric.split(".", 1)
    value = digest.get(section)
    if not isinstance(value, Mapping):
        return None
    return value.get(metric)


def _iter_metrics() -> list[str]:
    return [
        f"{section}.{metric}"
        for section in REQUIRED_SECTIONS
        for metric in REQUIRED_METRICS[section]
    ]


def _diff_metric(golden: Any, candidate: Any) -> dict[str, Any]:
    if not isinstance(golden, (int, float)) or not isinstance(candidate, (int, float)):
        return {
            "golden": golden,
            "candidate": candidate,
            "diff": None,
            "relative_diff": None,
        }
    diff = float(candidate) - float(golden)
    rel = None if float(golden) == 0.0 else diff / abs(float(golden))
    return {
        "golden": golden,
        "candidate": candidate,
        "diff": diff,
        "relative_diff": rel,
    }


def _evaluate_metric(metric: str, golden: Any, candidate: Any, tolerance: Mapping[str, Any]) -> str:
    if candidate is None or golden is None:
        return "fail"
    if tolerance.get("zero_fail") and isinstance(candidate, (int, float)) and candidate <= 0:
        return "fail"
    if not isinstance(golden, (int, float)) or not isinstance(candidate, (int, float)):
        return "pass" if golden == candidate else "warn"
    abs_diff = abs(float(candidate) - float(golden))
    rel_diff = 0.0 if float(golden) == 0.0 else abs_diff / abs(float(golden))
    if "fail_abs" in tolerance and abs_diff > float(tolerance["fail_abs"]):
        return "fail"
    if "fail_rel" in tolerance and rel_diff > float(tolerance["fail_rel"]):
        return "fail"
    if "warn_abs" in tolerance and abs_diff > float(tolerance["warn_abs"]):
        return "warn"
    if "warn_rel" in tolerance and rel_diff > float(tolerance["warn_rel"]):
        return "warn"
    return "pass"


def compare_replay_digest(
    golden: Mapping[str, Any],
    candidate: Mapping[str, Any],
    tolerances: Mapping[str, Mapping[str, Any]] | None = None,
) -> ReplayDigestComparison:
    tolerances = tolerances or DEFAULT_TOLERANCES
    missing_metrics: list[str] = []
    tolerance_failures: list[str] = []
    metric_diffs: dict[str, dict[str, Any]] = {}
    status = "pass"

    for digest_name, digest in (("golden", golden), ("candidate", candidate)):
        errors = validate_replay_digest(digest)
        for error in errors:
            missing_metrics.append(f"{digest_name}:{error}")
        if errors:
            status = "fail"

    for metric in _iter_metrics():
        golden_value = _metric_value(golden, metric)
        candidate_value = _metric_value(candidate, metric)
        metric_diffs[metric] = _diff_metric(golden_value, candidate_value)
        if golden_value is None or candidate_value is None:
            missing_metrics.append(metric)
            status = "fail"
            continue
        metric_status = _evaluate_metric(metric, golden_value, candidate_value, tolerances.get(metric, {}))
        metric_diffs[metric]["status"] = metric_status
        status = _combine_status(status, metric_status)
        if metric_status in {"warn", "fail"}:
            tolerance_failures.append(metric)

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "metric_diffs": metric_diffs,
        "missing_metrics": sorted(set(missing_metrics)),
        "tolerance_failures": sorted(set(tolerance_failures)),
        "source": {
            "golden_record": golden.get("source_record"),
            "candidate_record": candidate.get("source_record"),
            "golden_variant_id": golden.get("apollo_variant_id"),
            "candidate_variant_id": candidate.get("apollo_variant_id"),
        },
    }
    return ReplayDigestComparison(report)


def write_replay_comparison_report(
    out_dir: str | Path,
    comparison: ReplayDigestComparison | Mapping[str, Any],
    *,
    golden_path: str | Path | None = None,
    candidate_path: str | Path | None = None,
) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report = comparison.report if isinstance(comparison, ReplayDigestComparison) else dict(comparison)
    report_path = output_dir / "replay_comparison_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    outputs = {"report": str(report_path)}
    if golden_path is not None:
        dst = output_dir / "golden_digest.json"
        shutil.copyfile(golden_path, dst)
        outputs["golden_digest"] = str(dst)
    if candidate_path is not None:
        dst = output_dir / "candidate_digest.json"
        shutil.copyfile(candidate_path, dst)
        outputs["candidate_digest"] = str(dst)
    return outputs
