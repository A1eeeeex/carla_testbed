from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Mapping

from carla_testbed.analysis.artifact_completeness import (
    check_run_artifact_completeness,
    write_run_artifact_completeness_report,
)


PHASE1_ARTIFACT_NORMALIZATION_SCHEMA_VERSION = "phase1_artifact_normalization.v1"


def normalize_phase1_artifacts(run_dir: str | Path) -> dict[str, Any]:
    """Expose legacy nested artifacts on the Phase 1 canonical run surface.

    Some compatibility backends still write a legacy run directory inside the
    declared Phase 1 run directory. Phase 1 analyzers intentionally consume the
    declared run root, so a unique nested `timeseries.*` can be promoted there.
    This is evidence-surface normalization only; it does not rewrite behavior
    status or turn a timeout/failure into success.
    """

    root = Path(run_dir).expanduser()
    warnings: list[str] = []
    promoted_artifacts: list[dict[str, str]] = []
    status = "missing"
    try:
        if root.exists() and not root.is_dir():
            status = "error"
            source_artifacts = []
            warnings.append("run_dir_is_not_directory")
        else:
            root.mkdir(parents=True, exist_ok=True)
            timeseries = _promote_unique_timeseries(root)
            status = str(timeseries["status"])
            source_artifacts = list(timeseries["source_artifacts"])
            promoted_artifacts.extend(timeseries["promoted_artifacts"])
            warnings.extend(timeseries["warnings"])
            for spec in _OPTIONAL_PROMOTION_SPECS:
                optional = _promote_unique_relative_file(root, spec)
                promoted_artifacts.extend(optional["promoted_artifacts"])
                warnings.extend(optional["warnings"])
    except Exception as exc:  # pragma: no cover - defensive artifact path
        status = "error"
        source_artifacts = []
        warnings.append(f"normalization_error:{type(exc).__name__}:{exc}")

    report = {
        "schema_version": PHASE1_ARTIFACT_NORMALIZATION_SCHEMA_VERSION,
        "run_dir": str(root),
        "status": status,
        "canonical_surface": "timeseries.*",
        "accepted_formats": ["timeseries.csv", "timeseries.jsonl"],
        "source_artifacts": source_artifacts,
        "promoted_artifacts": promoted_artifacts,
        "warnings": warnings,
        "claim_boundary": (
            "phase1_artifact_normalization_only_exposes_existing_legacy_outputs; "
            "it_does_not_change_runtime_behavior_or_mark_failures_successful"
        ),
    }
    if root.is_dir():
        try:
            _write_report(root, report)
        except Exception as exc:  # pragma: no cover - report write is secondary
            report["warnings"] = [*list(report.get("warnings") or []), f"report_write_error:{type(exc).__name__}:{exc}"]
    return report


def ensure_phase1_comparison_artifacts(run_dir: str | Path) -> dict[str, Any]:
    """Write minimal Phase 1 comparison artifacts after runtime attempts.

    This helper fills backend-neutral report surfaces that can be derived from
    existing run evidence. It never marks behavior successful; it only makes
    explicit whether the declared run root has enough Phase 1 artifacts for
    ScenarioComparison.
    """

    root = Path(run_dir).expanduser()
    warnings: list[str] = []
    outputs: dict[str, str] = {}
    if not root.is_dir():
        return {
            "schema_version": "phase1_comparison_artifact_surface.v1",
            "run_dir": str(root),
            "status": "error",
            "warnings": ["run_dir_is_not_directory"],
            "outputs": {},
        }

    route_v_t_gap = _ensure_route_only_v_t_gap(root)
    if route_v_t_gap.get("written"):
        outputs["v_t_gap_report"] = str(root / "analysis" / "v_t_gap" / "v_t_gap_report.json")
    warnings.extend(route_v_t_gap.get("warnings") or [])

    completeness = check_run_artifact_completeness(root, profile="phase1")
    canonical_outputs = write_run_artifact_completeness_report(
        completeness,
        root / "analysis" / "artifact_completeness",
    )
    outputs.update(canonical_outputs)
    legacy_path = root / "analysis" / "phase1_status" / "artifact_completeness.json"
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_payload = {
        "schema_version": "phase1_artifact_completeness.v1",
        "status": completeness.get("status"),
        "artifact_complete": completeness.get("artifact_complete"),
        "missing_artifacts": list(completeness.get("missing_artifacts") or []),
        "warnings": list(completeness.get("warnings") or []),
        "source_profile": "run_artifact_completeness_phase1",
        "source_artifact": canonical_outputs.get("artifact_completeness_report"),
        "claim_boundary": (
            "phase1 artifact completeness supports ScenarioComparison only; "
            "it does not prove backend behavior success or natural driving."
        ),
    }
    legacy_path.write_text(json.dumps(legacy_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    outputs["phase1_status_artifact_completeness"] = str(legacy_path)

    return {
        "schema_version": "phase1_comparison_artifact_surface.v1",
        "run_dir": str(root),
        "status": completeness.get("status"),
        "artifact_complete": completeness.get("artifact_complete"),
        "v_t_gap_status": route_v_t_gap.get("status"),
        "missing_artifacts": list(completeness.get("missing_artifacts") or []),
        "warnings": warnings,
        "outputs": outputs,
        "claim_boundary": (
            "derived comparison artifacts only expose existing run evidence; "
            "they do not change runtime behavior or mark failures successful."
        ),
    }


_OPTIONAL_PROMOTION_SPECS = [
    {
        "name": "events",
        "root_rel": Path("events.jsonl"),
        "patterns": ("**/events.jsonl",),
    },
    {
        "name": "config_resolved",
        "root_rel": Path("config.resolved.yaml"),
        "patterns": ("**/config.resolved.yaml",),
    },
    {
        "name": "control_apply_trace",
        "root_rel": Path("artifacts/control_apply_trace.jsonl"),
        "patterns": ("**/artifacts/control_apply_trace.jsonl",),
    },
    {
        "name": "apollo_control_raw",
        "root_rel": Path("artifacts/apollo_control_raw.jsonl"),
        "patterns": ("**/artifacts/apollo_control_raw.jsonl",),
    },
    {
        "name": "ego_control_trace",
        "root_rel": Path("artifacts/ego_control_trace.jsonl"),
        "patterns": ("**/artifacts/ego_control_trace.jsonl",),
    },
]


def _promote_unique_timeseries(root: Path) -> dict[str, Any]:
    existing = _root_timeseries(root)
    if existing is not None:
        return {
            "status": "already_present",
            "source_artifacts": [str(existing)],
            "promoted_artifacts": [],
            "warnings": [],
        }
    candidates = _nested_timeseries_candidates(root)
    if len(candidates) == 1:
        source = candidates[0]
        destination = root / source.name
        shutil.copy2(source, destination)
        return {
            "status": "promoted",
            "source_artifacts": [str(source)],
            "promoted_artifacts": [{"name": "timeseries", "source": str(source), "destination": str(destination)}],
            "warnings": [],
        }
    if len(candidates) > 1:
        return {
            "status": "ambiguous",
            "source_artifacts": [str(path) for path in candidates],
            "promoted_artifacts": [],
            "warnings": ["multiple_nested_timeseries_candidates"],
        }
    return {
        "status": "missing",
        "source_artifacts": [],
        "promoted_artifacts": [],
        "warnings": ["timeseries_missing_from_root_and_nested_legacy_runs"],
    }


def _promote_unique_relative_file(root: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    destination = root / Path(spec["root_rel"])
    if destination.exists():
        return {"promoted_artifacts": [], "warnings": []}
    candidates = _nested_candidates_for_patterns(root, [str(pattern) for pattern in spec["patterns"]])
    if len(candidates) == 1:
        source = candidates[0]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return {
            "promoted_artifacts": [
                {"name": str(spec["name"]), "source": str(source), "destination": str(destination)}
            ],
            "warnings": [],
        }
    if len(candidates) > 1:
        return {"promoted_artifacts": [], "warnings": [f"multiple_nested_{spec['name']}_candidates"]}
    return {"promoted_artifacts": [], "warnings": []}


def _root_timeseries(root: Path) -> Path | None:
    for name in ("timeseries.csv", "timeseries.jsonl"):
        path = root / name
        if path.exists():
            return path
    return None


def _nested_timeseries_candidates(root: Path) -> list[Path]:
    return _nested_candidates_for_patterns(root, ["**/timeseries.csv", "**/timeseries.jsonl"])


def _nested_candidates_for_patterns(root: Path, patterns: list[str]) -> list[Path]:
    candidates: dict[Path, Path] = {}
    for pattern in patterns:
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            if path.parent == root or path == root / path.name:
                continue
            if _inside_normalization_report(path):
                continue
            if "analysis" in path.parts and "phase1_artifact_normalization" in path.parts:
                continue
            try:
                resolved = path.resolve()
            except OSError:
                resolved = path
            candidates.setdefault(resolved, path)
    return sorted(candidates.values(), key=lambda item: (item.name, str(item)))


def _ensure_route_only_v_t_gap(root: Path) -> dict[str, Any]:
    path = root / "analysis" / "v_t_gap" / "v_t_gap_report.json"
    if path.exists():
        return {"status": "already_present", "written": False, "warnings": []}
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    phase1_status = _read_json(root / "analysis" / "phase1_status" / "phase1_status.json")
    if _phase1_target_required(manifest=manifest, summary=summary, phase1_status=phase1_status):
        return {"status": "target_required", "written": False, "warnings": []}
    scenario_id = (
        manifest.get("scenario_case")
        or manifest.get("scenario_id")
        or phase1_status.get("scenario_case")
        or phase1_status.get("scenario_id")
        or summary.get("scenario_id")
    )
    report = {
        "schema_version": "v_t_gap.v1",
        "status": "not_applicable",
        "run_id": manifest.get("run_id") or phase1_status.get("run_id") or root.name,
        "scenario_id": scenario_id,
        "scenario_case": scenario_id,
        "target_actor_required": False,
        "target_actor_contract": {
            "status": "not_required",
            "required": False,
            "source": "phase1_route_only_artifact_surface",
        },
        "rows": [],
        "missing_fields": [],
        "warnings": ["route_only_scenario_has_no_target_gap_metric"],
        "claim_boundary": "v_t_gap_not_applicable_for_route_only_phase1_run",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"status": "not_applicable", "written": True, "warnings": []}


def _phase1_target_required(
    *,
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    phase1_status: Mapping[str, Any],
) -> bool:
    target_contract = phase1_status.get("target_actor_contract")
    if not isinstance(target_contract, Mapping):
        target_contract = manifest.get("target_actor_contract")
    if isinstance(target_contract, Mapping):
        if target_contract.get("required") is False:
            return False
        if target_contract.get("status") == "not_required":
            return False
        if target_contract.get("status") == "resolved":
            return True
    scenario = str(
        manifest.get("scenario_case")
        or manifest.get("scenario_class")
        or manifest.get("scenario_id")
        or phase1_status.get("scenario_case")
        or phase1_status.get("scenario_class")
        or phase1_status.get("scenario_id")
        or summary.get("scenario_class")
        or summary.get("scenario_id")
        or ""
    )
    if any(token in scenario for token in ("follow_stop", "lead_", "cut_in", "cut_out", "hard_brake")):
        return True
    return False


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _inside_normalization_report(path: Path) -> bool:
    return "phase1_artifact_normalization" in path.parts


def _write_report(root: Path, report: dict[str, Any]) -> None:
    out_dir = root / "analysis" / "phase1_artifact_normalization"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "phase1_artifact_normalization_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Phase 1 Artifact Normalization",
        "",
        f"Status: `{report.get('status')}`",
        "",
        f"Canonical surface: `{report.get('canonical_surface')}`",
        "",
        f"Claim boundary: `{report.get('claim_boundary')}`",
    ]
    promoted = report.get("promoted_artifacts")
    if isinstance(promoted, list) and promoted:
        lines.extend(["", "## Promoted Artifacts", ""])
        for item in promoted:
            if not isinstance(item, dict):
                continue
            lines.append(f"- `{item.get('source')}` -> `{item.get('destination')}`")
    warnings = report.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.extend(["", "## Warnings", ""])
        for warning in warnings:
            lines.append(f"- `{warning}`")
    (out_dir / "phase1_artifact_normalization_summary.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
