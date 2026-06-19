from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

from .phase1_scenario_scaffold import write_phase1_scenario_scaffold


PHASE1_APOLLO_DISPATCH_SCAFFOLD_BATCH_SCHEMA_VERSION = "phase1_apollo_dispatch_scaffold_batch.v1"

DEFAULT_DYNAMIC_APOLLO_FIXED_SCENE_SCENARIOS = (
    "baguang_lead_accel_40_to_70_20m",
    "baguang_lead_decel_70_to_40_20m",
    "baguang_cut_in_35kph_left_to_right_10m",
    "cut_out_097",
    "baguang_lead_hard_brake_70_to_0_20m",
)


def write_apollo_dispatch_scaffold_batch(
    *,
    out_dir: str | Path,
    scenarios: Iterable[str] | None = None,
    repo_root: str | Path = ".",
    bridge_config: str | None = "configs/io/examples/phase1_baguang_apollo_fixed_scene_bridge.yaml",
    algorithm: str = "apollo/apollo10_carla_gt",
    recording: str = "none",
    gate: str = "scenario_validation",
    traffic: str = "none",
) -> dict[str, Any]:
    """Write CI-safe Apollo fixed-scene dispatch scaffolds for multiple cases."""

    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    scenario_list = list(scenarios or DEFAULT_DYNAMIC_APOLLO_FIXED_SCENE_SCENARIOS)
    rows: list[dict[str, Any]] = []
    for scenario in scenario_list:
        run_dir = output / _safe_run_dir_name(scenario)
        try:
            result = write_phase1_scenario_scaffold(
                scenario=scenario,
                backend="apollo_cyberrt",
                algorithm=algorithm,
                recording=recording,
                gate=gate,
                traffic=traffic,
                bridge_config=bridge_config,
                run_dir=run_dir,
                repo_root=repo_root,
            )
            row = _row_from_scaffold_result(scenario, run_dir, result)
        except Exception as exc:  # noqa: BLE001 - batch reports per-case compile/setup failures
            row = {
                "scenario": scenario,
                "run_dir": str(run_dir),
                "scaffold_status": "error",
                "preflight_status": "error",
                "phase1_status": "invalid",
                "failure_reason": f"{type(exc).__name__}: {exc}",
                "dispatch_status": "missing",
                "dispatch_mode": None,
                "runtime_migration_requirements": [],
                "claim_boundary": "batch scaffold failure is setup evidence, not backend behavior evidence",
            }
        rows.append(row)
    status = "ok" if all(row["scaffold_status"] in {"written", "invalid_written"} for row in rows) else "partial"
    report = {
        "schema_version": PHASE1_APOLLO_DISPATCH_SCAFFOLD_BATCH_SCHEMA_VERSION,
        "status": status,
        "out_dir": str(output),
        "scenarios": scenario_list,
        "rows": rows,
        "claim_boundary": (
            "This batch writes CI-safe Apollo dispatch scaffolds only. Invalid/backend_not_ready "
            "rows are setup evidence and must not be counted as Apollo backend losses."
        ),
    }
    manifest_path = output / "phase1_apollo_dispatch_scaffold_batch_manifest.json"
    matrix_path = output / "phase1_apollo_dispatch_scaffold_matrix.csv"
    manifest_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_matrix(matrix_path, rows)
    report["outputs"] = {
        "manifest": str(manifest_path),
        "matrix": str(matrix_path),
    }
    manifest_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _row_from_scaffold_result(scenario: str, run_dir: Path, result: dict[str, Any]) -> dict[str, Any]:
    dispatch = _read_json(
        run_dir
        / "analysis"
        / "phase1_apollo_fixed_scene_dispatch"
        / "phase1_apollo_fixed_scene_dispatch_report.json"
    )
    preflight = _read_json(run_dir / "preflight.json")
    return {
        "scenario": scenario,
        "scenario_id": result.get("scenario_id"),
        "scenario_class": result.get("scenario_class"),
        "run_dir": str(run_dir),
        "scaffold_status": "written" if result.get("exit_code") == 0 else "invalid_written",
        "preflight_status": result.get("preflight_status"),
        "phase1_status": result.get("phase1_status"),
        "failure_reason": result.get("failure_reason"),
        "dispatch_status": dispatch.get("status") or "missing",
        "dispatch_mode": dispatch.get("dispatch_mode"),
        "starts_runtime": dispatch.get("starts_runtime"),
        "commands_present": dispatch.get("commands_present"),
        "blocking_reasons": list(dispatch.get("blocking_reasons") or []),
        "runtime_migration_requirements": list(dispatch.get("runtime_migration_requirements") or []),
        "preflight_reasons": list(preflight.get("reasons") or []),
        "claim_boundary": "dispatch scaffold row is not online behavior evidence",
    }


def _write_matrix(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "scenario",
        "scenario_id",
        "scenario_class",
        "run_dir",
        "scaffold_status",
        "preflight_status",
        "phase1_status",
        "failure_reason",
        "dispatch_status",
        "dispatch_mode",
        "starts_runtime",
        "commands_present",
        "blocking_reasons",
        "runtime_migration_requirements",
        "preflight_reasons",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: ";".join(str(item) for item in value)
                    if isinstance(value := row.get(key), list)
                    else value
                    for key in fieldnames
                }
            )


def _safe_run_dir_name(scenario: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in scenario).strip("_") or "scenario"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}
