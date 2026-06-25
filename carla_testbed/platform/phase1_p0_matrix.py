from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from .phase1_pair_runner import Phase1PairRunResult, run_phase1_pair
from .registry import PlatformRegistry


PHASE1_P0_MATRIX_SCHEMA_VERSION = "phase1_p0_matrix.v1"


@dataclass(frozen=True)
class Phase1P0Scenario:
    canonical_case: str
    scenario: str
    rationale: str


DEFAULT_PHASE1_P0_SCENARIOS: tuple[Phase1P0Scenario, ...] = (
    Phase1P0Scenario(
        canonical_case="follow_stop_static",
        scenario="baguang/follow_stop_static_300m_spawn2m",
        rationale="Baguang static lead follow-stop with known 2m spawn mitigation.",
    ),
    Phase1P0Scenario(
        canonical_case="lead_decel_accel",
        scenario="baguang/lead_decel_70_to_40_20m",
        rationale=(
            "Representative Baguang lead-speed-change case with existing online "
            "delivery evidence; it covers the deceleration half of the canonical family."
        ),
    ),
    Phase1P0Scenario(
        canonical_case="cut_in_simple",
        scenario="baguang/cut_in_35kph_left_to_right_10m",
        rationale="Baguang adjacent-lane target activates through a scripted cut-in.",
    ),
    Phase1P0Scenario(
        canonical_case="lane_keep_straight",
        scenario="town01/lane_keep_097",
        rationale="Town01 straight-lane baseline route.",
    ),
    Phase1P0Scenario(
        canonical_case="lane_keep_curve",
        scenario="town01/curve217_diagnostic",
        rationale="Town01 curve diagnostic route; remains diagnostic unless claim-grade route/reference evidence exists.",
    ),
)


@dataclass(frozen=True)
class Phase1P0MatrixResult:
    matrix_id: str
    out_dir: Path
    manifest_path: Path
    csv_path: Path
    rows: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": PHASE1_P0_MATRIX_SCHEMA_VERSION,
            "matrix_id": self.matrix_id,
            "out_dir": str(self.out_dir),
            "manifest_path": str(self.manifest_path),
            "csv_path": str(self.csv_path),
            "rows": list(self.rows),
        }


def run_phase1_p0_matrix(
    *,
    out_dir: str | Path,
    matrix_id: str | None = None,
    scenarios: Sequence[Phase1P0Scenario] | None = None,
    dry_run: bool = False,
    timeout_s: float | None = None,
    continue_on_failure: bool = True,
    apollo_profile: str = "apollo/apollo10_carla_gt",
    planning_profile: str = "builtin/simple_acc_route_follower",
    apollo_platform: str = "apollo_cyberrt",
    planning_platform: str = "carla_builtin",
    recording: str = "metrics",
    gate: str = "scenario_validation",
    registry: PlatformRegistry | None = None,
) -> Phase1P0MatrixResult:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    resolved_matrix_id = matrix_id or f"phase1_p0_matrix_{int(time.time())}"
    pairs_dir = output / "pairs"
    pairs_dir.mkdir(parents=True, exist_ok=True)
    resolved_scenarios = list(scenarios or DEFAULT_PHASE1_P0_SCENARIOS)
    registry = registry or PlatformRegistry(repo_root=".")

    rows: list[dict[str, Any]] = []
    for item in resolved_scenarios:
        pair_id = _pair_id(resolved_matrix_id, item.canonical_case)
        pair_out = pairs_dir / item.canonical_case
        try:
            pair = run_phase1_pair(
                scenario=item.scenario,
                out_dir=pair_out,
                pair_id=pair_id,
                apollo_profile=apollo_profile,
                planning_profile=planning_profile,
                apollo_platform=apollo_platform,
                planning_platform=planning_platform,
                recording=recording,
                gate=gate,
                dry_run=dry_run,
                timeout_s=timeout_s,
                registry=registry,
            )
        except Exception as exc:
            row = _error_row(item, pair_id=pair_id, pair_out=pair_out, error=exc)
            rows.append(row)
            if not continue_on_failure:
                _write_outputs(
                    output,
                    matrix_id=resolved_matrix_id,
                    rows=rows,
                    dry_run=dry_run,
                    timeout_s=timeout_s,
                    continue_on_failure=continue_on_failure,
                )
                raise
            continue
        rows.append(_row_from_pair(item, pair))

    manifest_path, csv_path = _write_outputs(
        output,
        matrix_id=resolved_matrix_id,
        rows=rows,
        dry_run=dry_run,
        timeout_s=timeout_s,
        continue_on_failure=continue_on_failure,
    )
    return Phase1P0MatrixResult(
        matrix_id=resolved_matrix_id,
        out_dir=output,
        manifest_path=manifest_path,
        csv_path=csv_path,
        rows=rows,
    )


def _pair_id(matrix_id: str, canonical_case: str) -> str:
    return f"{_slug(matrix_id)}__{_slug(canonical_case)}"


def _row_from_pair(item: Phase1P0Scenario, pair: Phase1PairRunResult) -> dict[str, Any]:
    statuses = [result.status for result in pair.execution_results]
    exit_codes = [result.exit_code for result in pair.execution_results]
    if statuses and all(status == "dry_run" for status in statuses):
        row_status = "dry_run"
    elif exit_codes and all(code == 0 for code in exit_codes):
        row_status = "completed"
    else:
        row_status = "partial_or_failed"
    return {
        "canonical_case": item.canonical_case,
        "scenario": item.scenario,
        "rationale": item.rationale,
        "pair_id": pair.pair_id,
        "pair_dir": str(pair.out_dir),
        "pair_manifest_path": str(pair.manifest_path),
        "run_dirs": [str(path) for path in pair.run_dirs],
        "plan_paths": [str(path) for path in pair.plan_paths],
        "execution_statuses": statuses,
        "exit_codes": exit_codes,
        "comparison_outputs": dict(pair.comparison_outputs),
        "status": row_status,
        "error": None,
    }


def _error_row(
    item: Phase1P0Scenario,
    *,
    pair_id: str,
    pair_out: Path,
    error: Exception,
) -> dict[str, Any]:
    return {
        "canonical_case": item.canonical_case,
        "scenario": item.scenario,
        "rationale": item.rationale,
        "pair_id": pair_id,
        "pair_dir": str(pair_out),
        "pair_manifest_path": None,
        "run_dirs": [],
        "plan_paths": [],
        "execution_statuses": [],
        "exit_codes": [],
        "comparison_outputs": {},
        "status": "error",
        "error": f"{error.__class__.__name__}: {error}",
    }


def _write_outputs(
    output: Path,
    *,
    matrix_id: str,
    rows: list[dict[str, Any]],
    dry_run: bool,
    timeout_s: float | None,
    continue_on_failure: bool,
) -> tuple[Path, Path]:
    manifest_path = output / "phase1_p0_matrix_manifest.json"
    csv_path = output / "phase1_p0_matrix.csv"
    manifest = {
        "schema_version": PHASE1_P0_MATRIX_SCHEMA_VERSION,
        "matrix_id": matrix_id,
        "created_wall_time_s": time.time(),
        "dry_run": bool(dry_run),
        "timeout_s": timeout_s,
        "continue_on_failure": bool(continue_on_failure),
        "scenario_count": len(rows),
        "rows": rows,
        "summary": _summary(rows),
        "claim_boundary": (
            "Phase 1 P0 matrix output is orchestration evidence only. It does not prove Apollo "
            "behavior success, no-assist operation, or natural driving. Invalid runs are setup/evidence "
            "failures and must not be counted as backend behavior losses."
        ),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "canonical_case",
                "scenario",
                "pair_id",
                "status",
                "pair_dir",
                "pair_manifest_path",
                "execution_statuses",
                "exit_codes",
                "error",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "canonical_case": row["canonical_case"],
                    "scenario": row["scenario"],
                    "pair_id": row["pair_id"],
                    "status": row["status"],
                    "pair_dir": row["pair_dir"],
                    "pair_manifest_path": row.get("pair_manifest_path"),
                    "execution_statuses": json.dumps(row.get("execution_statuses") or []),
                    "exit_codes": json.dumps(row.get("exit_codes") or []),
                    "error": row.get("error"),
                }
            )
    return manifest_path, csv_path


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    statuses: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "unknown")
        statuses[status] = statuses.get(status, 0) + 1
    return {
        "status_counts": statuses,
        "all_pairs_materialized": bool(rows) and all(row.get("pair_manifest_path") for row in rows),
        "all_dry_run": bool(rows) and all(row.get("status") == "dry_run" for row in rows),
        "any_error": any(row.get("status") == "error" for row in rows),
    }


def _slug(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in value.strip().lower())
    return "_".join(part for part in cleaned.split("_") if part) or "phase1"
