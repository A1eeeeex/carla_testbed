from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.phase1_status import classify_phase1_run

SCENARIO_COMPARISON_SCHEMA_VERSION = "phase1_comparison.v1"


def compare_scenario_runs(run_dirs: Sequence[str | Path]) -> dict[str, Any]:
    runs = [_run_entry(Path(path).expanduser()) for path in run_dirs]
    scenario_ids = sorted({str(run.get("scenario_id")) for run in runs if run.get("scenario_id")})
    invalid_runs = [run for run in runs if run.get("phase1_status") == "invalid"]
    evaluable_runs = [run for run in runs if run.get("phase1_status") != "invalid"]
    if not runs:
        comparison_status = "invalid"
        reason = "no_runs_provided"
    elif len(scenario_ids) > 1:
        comparison_status = "invalid"
        reason = "scenario_id_mismatch"
    elif invalid_runs and evaluable_runs:
        comparison_status = "partially_evaluable"
        reason = "some_runs_invalid"
    elif invalid_runs:
        comparison_status = "invalid"
        reason = "all_runs_invalid"
    else:
        comparison_status = "comparable"
        reason = None
    return {
        "schema_version": SCENARIO_COMPARISON_SCHEMA_VERSION,
        "scenario_id": scenario_ids[0] if len(scenario_ids) == 1 else None,
        "comparison_status": comparison_status,
        "reason": reason,
        "participating_runs": runs,
        "invalid_runs": [run for run in runs if run.get("phase1_status") == "invalid"],
        "evaluable_runs": [run for run in runs if run.get("phase1_status") != "invalid"],
        "backend_results": _backend_results(runs, comparison_status=comparison_status),
        "claim_boundary": (
            "ScenarioComparison compares evaluable Phase 1 runs only; invalid runs are setup/artifact issues "
            "and are not backend losses."
        ),
    }


def write_scenario_comparison(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "comparison_manifest.json"
    summary_path = output / "comparison_summary.json"
    md_path = output / "comparison_summary.md"
    curves_dir = output / "comparison_curves"
    curves_path = curves_dir / "v_t_gap.csv"
    manifest = {
        "schema_version": "phase1_scenario_comparison_manifest.v1",
        "scenario_id": report.get("scenario_id"),
        "comparison_status": report.get("comparison_status"),
        "participating_run_dirs": [run.get("run_dir") for run in report.get("participating_runs") or []],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_summary_markdown(report), encoding="utf-8")
    curve_written = _write_combined_v_t_gap(report, curves_path)
    outputs = {"manifest": str(manifest_path), "summary": str(summary_path), "markdown": str(md_path)}
    if curve_written:
        outputs["v_t_gap_csv"] = str(curves_path)
    return outputs


def _run_entry(run_dir: Path) -> dict[str, Any]:
    manifest = _read_json(run_dir / "manifest.json")
    phase1_status = _read_json(run_dir / "analysis" / "phase1_status" / "phase1_status.json")
    if not phase1_status:
        phase1_status = classify_phase1_run(run_dir)
    v_t_gap = _read_json(run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json")
    return {
        "run_dir": str(run_dir),
        "run_id": manifest.get("run_id") or phase1_status.get("run_id") or run_dir.name,
        "scenario_id": manifest.get("scenario_id") or phase1_status.get("scenario_id"),
        "backend": manifest.get("backend") or phase1_status.get("backend"),
        "backend_type": manifest.get("backend_type") or phase1_status.get("backend_type"),
        "phase1_status": phase1_status.get("status"),
        "failure_reason": phase1_status.get("failure_reason"),
        "evaluable": phase1_status.get("evaluable"),
        "v_t_gap_status": v_t_gap.get("status") if v_t_gap else phase1_status.get("v_t_gap_status"),
        "artifact_paths": {
            "manifest": str(run_dir / "manifest.json"),
            "summary": str(run_dir / "summary.json"),
            "phase1_status": str(run_dir / "analysis" / "phase1_status" / "phase1_status.json"),
            "v_t_gap": str(run_dir / "analysis" / "v_t_gap" / "v_t_gap_report.json"),
        },
    }


def _backend_results(runs: list[dict[str, Any]], *, comparison_status: str) -> list[dict[str, Any]]:
    results = []
    for run in runs:
        evaluable = bool(run.get("evaluable"))
        results.append(
            {
                "backend": run.get("backend"),
                "backend_type": run.get("backend_type"),
                "run_id": run.get("run_id"),
                "phase1_status": run.get("phase1_status"),
                "failure_reason": run.get("failure_reason"),
                "counts_as_backend_loss": (
                    comparison_status == "comparable" and evaluable and run.get("phase1_status") == "failed"
                ),
            }
        )
    return results


def _summary_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Scenario Comparison",
        "",
        f"Scenario: `{report.get('scenario_id')}`",
        f"Status: `{report.get('comparison_status')}`",
        f"Reason: `{report.get('reason')}`",
        "",
        "| Run | Backend | Status | Reason | Backend loss? |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in report.get("backend_results") or []:
        if isinstance(item, Mapping):
            lines.append(
                f"| {item.get('run_id')} | {item.get('backend')} | {item.get('phase1_status')} | "
                f"{item.get('failure_reason')} | {item.get('counts_as_backend_loss')} |"
            )
    lines.append("")
    lines.append(str(report.get("claim_boundary") or ""))
    return "\n".join(lines) + "\n"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _write_combined_v_t_gap(report: Mapping[str, Any], output_path: Path) -> bool:
    rows: list[dict[str, Any]] = []
    for run in report.get("participating_runs") or []:
        if not isinstance(run, Mapping):
            continue
        csv_path = Path(str((run.get("artifact_paths") or {}).get("v_t_gap", ""))).with_name("v_t_gap.csv")
        if not csv_path.exists():
            continue
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                enriched = dict(row)
                enriched["run_id"] = run.get("run_id")
                enriched["backend"] = run.get("backend")
                enriched["backend_type"] = run.get("backend_type")
                rows.append(enriched)
    if not rows:
        return False
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "backend",
        "backend_type",
        "sim_time_s",
        "ego_speed_mps",
        "target_speed_mps",
        "gap_m",
        "relative_speed_mps",
        "target_actor_id",
        "target_actor_role",
        "gap_method",
        "gap_degraded",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
    return True
