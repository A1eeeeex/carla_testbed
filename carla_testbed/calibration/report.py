from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .control_actuation import CALIBRATION_TRIAL_FIELDS, analyze_control_actuation_trials, load_calibration_trials_csv
from .profile import CalibrationProfile, REQUIRED_GATE_ROUTE_IDS, load_calibration_profile

CALIBRATION_REPORT_SCHEMA_VERSION = "calibration_report.v1"


def _read_gate_results(path: str | Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    gate_path = Path(path).expanduser()
    if not gate_path.exists():
        return {}
    payload = json.loads(gate_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        return {}
    if isinstance(payload.get("gates"), Mapping):
        payload = payload["gates"]
    gates: dict[str, dict[str, Any]] = {}
    for route_id, value in payload.items():
        if isinstance(value, Mapping):
            gates[str(route_id)] = dict(value)
        else:
            gates[str(route_id)] = {"status": str(value)}
    return gates


def _build_no_regression(gate_results: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    gates: dict[str, dict[str, Any]] = {}
    failed_gates: list[str] = []
    missing_gates: list[str] = []
    for route_id in sorted(REQUIRED_GATE_ROUTE_IDS):
        raw = dict(gate_results.get(route_id) or {})
        status = str(raw.get("status") or "missing")
        gate = {
            "route_id": route_id,
            "status": status,
            "required": True,
            "reason": raw.get("reason"),
            "summary_path": raw.get("summary_path"),
            "route_health_path": raw.get("route_health_path"),
        }
        gates[route_id] = gate
        if status == "missing":
            missing_gates.append(route_id)
        elif status != "pass":
            failed_gates.append(route_id)
    return {
        "gates": gates,
        "promotion_allowed": not failed_gates and not missing_gates,
        "failed_gates": failed_gates,
        "missing_gates": missing_gates,
    }


def _build_recommendation(results: Mapping[str, Any], no_regression: Mapping[str, Any]) -> dict[str, Any]:
    steer = results.get("steer_response") or {}
    gate_ok = bool(no_regression.get("promotion_allowed"))
    legacy_supported = steer.get("legacy_steer_scale_025_supported")
    keep_legacy = bool(legacy_supported) if gate_ok else None
    reason_parts = []
    if not gate_ok:
        reason_parts.append("097/217/031 no-regression gates are not all pass")
    if legacy_supported is True:
        reason_parts.append("steer trials support the current legacy steer_scale=0.25 interpretation")
    elif legacy_supported is False:
        reason_parts.append("steer trials do not support the current legacy steer_scale=0.25 interpretation")
    else:
        reason_parts.append("steer evidence is insufficient")
    return {
        "keep_legacy_steer_scale_025": keep_legacy,
        "enable_physical_mapping": False,
        "reason": "; ".join(reason_parts),
        "required_next_steps": [
            "collect real CARLA control-actuation trials",
            "pass 097/217/031 no-regression gates before any promotion",
            "keep physical mapping disabled unless a gated profile supports re-testing",
        ],
    }


def build_calibration_report(
    *,
    profile: CalibrationProfile,
    trials: Sequence[Mapping[str, Any]],
    trials_path: str | Path | None = None,
    gate_results_path: str | Path | None = None,
) -> dict[str, Any]:
    results, missing_fields, warnings = analyze_control_actuation_trials(trials, profile=profile)
    gate_results = _read_gate_results(gate_results_path)
    no_regression = _build_no_regression(gate_results)
    if not gate_results:
        warnings.append("gate_results missing; promotion is disabled")
    recommendation = _build_recommendation(results, no_regression)
    return {
        "schema_version": CALIBRATION_REPORT_SCHEMA_VERSION,
        "profile_id": profile.profile_id,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "inputs": {
            "profile_path": profile.source_path,
            "trials_path": None if trials_path is None else str(trials_path),
            "gate_results_path": None if gate_results_path is None else str(gate_results_path),
        },
        "environment": profile.environment,
        "vehicle": profile.vehicle,
        "results": results,
        "no_regression": no_regression,
        "recommendation": recommendation,
        "missing_fields": sorted(set(missing_fields)),
        "warnings": warnings,
    }


def _write_trials_copy(path: Path, trials: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CALIBRATION_TRIAL_FIELDS)
        writer.writeheader()
        for row in trials:
            writer.writerow({field: row.get(field) for field in CALIBRATION_TRIAL_FIELDS})


def write_calibration_report(
    *,
    out_dir: str | Path,
    report: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    plots_dir = output / "calibration_plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    report_path = output / "calibration_report.json"
    trials_path = output / "calibration_trials.csv"
    plots_readme = plots_dir / "README.md"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    _write_trials_copy(trials_path, trials)
    plots_readme.write_text(
        "# Calibration Plots\n\nPlots are not generated yet in the minimal offline pipeline.\n",
        encoding="utf-8",
    )
    return {
        "calibration_report_json": str(report_path),
        "calibration_trials_csv": str(trials_path),
        "calibration_plots_readme": str(plots_readme),
    }


def analyze_calibration_report(
    *,
    profile_path: str | Path,
    trials_path: str | Path,
    gate_results_path: str | Path | None = None,
    out_dir: str | Path | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    profile = load_calibration_profile(profile_path)
    trials = load_calibration_trials_csv(trials_path)
    report = build_calibration_report(
        profile=profile,
        trials=trials,
        trials_path=trials_path,
        gate_results_path=gate_results_path,
    )
    outputs: dict[str, str] = {}
    if out_dir is not None:
        outputs = write_calibration_report(out_dir=out_dir, report=report, trials=trials)
        source = Path(trials_path).expanduser()
        target = Path(outputs["calibration_trials_csv"])
        if source.exists() and source.resolve() != target.resolve():
            # Preserve exact input formatting when possible while still using
            # the fixed schema writer for generated rows in tests/fallbacks.
            shutil.copyfile(source, target)
    return report, outputs
