from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from carla_testbed.calibration.gates import build_gate_results_from_ab_report, write_gate_results
from carla_testbed.calibration.report import analyze_calibration_report
from carla_testbed.calibration.trial_extraction import (
    discover_timeseries_paths,
    extract_control_actuation_trials,
    load_timeseries_rows,
    write_calibration_trials_csv,
)
from carla_testbed.experiments.ab_manifest import ABManifest, ABRunRecord, load_ab_manifest

from .curve_pair_semantics import compare_curve_pair_files, render_curve_pair_summary
from .route_curve_artifact_gap import (
    analyze_route_curve_artifact_gap,
    write_route_curve_artifact_gap_report,
)
from .route_health_report import analyze_route_health_run_dir
from .town01_goal_audit import build_goal_audit, render_goal_audit_markdown
from .transport_ab import analyze_ab_manifest, write_ab_report

TOWN01_POSTPROCESS_SCHEMA_VERSION = "town01_postprocess.v1"


def _resolve(path_text: str | None, *, manifest_dir: Path) -> Path | None:
    if not path_text:
        return None
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    if path.exists():
        return path
    return manifest_dir / path


def _manifest_for_batch(batch_root: Path) -> Path:
    direct = batch_root / "ab_manifest.json"
    if direct.exists():
        return direct
    candidates = sorted(batch_root.rglob("ab_manifest.json"))
    if not candidates:
        raise FileNotFoundError(f"ab_manifest.json not found under {batch_root}")
    return candidates[0]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _analyze_batch(
    *,
    label: str,
    batch_root: Path,
    out_dir: Path,
) -> dict[str, Any]:
    manifest_path = _manifest_for_batch(batch_root)
    report = analyze_ab_manifest(manifest_path, batch_root=batch_root)
    analysis_dir = out_dir / "ab" / label
    outputs = write_ab_report(analysis_dir, report)
    return {
        "label": label,
        "batch_root": str(batch_root),
        "manifest_path": str(manifest_path),
        "report": report,
        "outputs": outputs,
    }


def _run_dirs_from_manifest(manifest_path: Path) -> list[tuple[ABRunRecord, Path]]:
    manifest: ABManifest = load_ab_manifest(manifest_path)
    manifest_dir = manifest_path.parent
    resolved: list[tuple[ABRunRecord, Path]] = []
    for run in manifest.runs:
        run_dir = _resolve(run.run_dir, manifest_dir=manifest_dir)
        if run_dir is not None:
            resolved.append((run, run_dir))
    return resolved


def _ensure_route_health(
    *,
    manifest_path: Path,
    refresh: bool,
    generated_root: Path,
) -> dict[str, Any]:
    generated: list[str] = []
    existing: list[str] = []
    missing_run_dirs: list[str] = []
    errors: list[dict[str, str]] = []
    route_health_by_route: dict[str, list[str]] = {}
    for run, run_dir in _run_dirs_from_manifest(manifest_path):
        if not run_dir.exists():
            missing_run_dirs.append(str(run_dir))
            continue
        route_health_path = run_dir / "analysis" / "route_health" / "route_health.json"
        if route_health_path.exists() and not refresh:
            existing.append(str(route_health_path))
        else:
            try:
                run_output_name = _safe_path_component(run.run_id or f"{run.backend}_{run.route_id}")
                result = analyze_route_health_run_dir(run_dir, out_dir=generated_root / run_output_name)
            except Exception as exc:  # noqa: BLE001 - postprocess should keep the batch inspectable.
                errors.append({"run_dir": str(run_dir), "error": str(exc)})
                continue
            route_health_path = Path(result["outputs"]["route_health_json"])
            generated.append(str(route_health_path))
        if route_health_path.exists():
            route_health_by_route.setdefault(run.route_id, []).append(str(route_health_path))
    return {
        "generated": generated,
        "existing": existing,
        "missing_run_dirs": missing_run_dirs,
        "errors": errors,
        "route_health_by_route": route_health_by_route,
    }


def _safe_path_component(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in ("-", "_", ".") else "_" for char in value.strip())
    return cleaned or "unknown_run"


def _first_file(root: Path, names: Sequence[str]) -> Path | None:
    for name in names:
        direct = root / name
        if direct.exists() and direct.is_file():
            return direct
    for name in names:
        candidates = [path for path in root.rglob(name) if path.is_file()]
        if candidates:
            return max(candidates, key=lambda path: path.stat().st_mtime)
    return None


def _ensure_route_curve_artifact_gap(
    *,
    manifest_path: Path,
    generated_root: Path,
) -> dict[str, Any]:
    generated: list[str] = []
    missing_run_dirs: list[str] = []
    errors: list[dict[str, str]] = []
    by_route: dict[str, list[str]] = {}
    for run, run_dir in _run_dirs_from_manifest(manifest_path):
        if not run_dir.exists():
            missing_run_dirs.append(str(run_dir))
            continue
        timeseries = _first_file(run_dir, ["timeseries.csv"])
        summary = _first_file(run_dir, ["summary.json"])
        try:
            report = analyze_route_curve_artifact_gap(timeseries, summary_json=summary)
            run_output_name = _safe_path_component(run.run_id or f"{run.backend}_{run.route_id}")
            outputs = write_route_curve_artifact_gap_report(
                report,
                generated_root / run_output_name,
            )
        except Exception as exc:  # noqa: BLE001 - postprocess should keep the batch inspectable.
            errors.append({"run_dir": str(run_dir), "error": str(exc)})
            continue
        path = outputs["route_curve_artifact_gap_report"]
        generated.append(path)
        by_route.setdefault(run.route_id, []).append(path)
    return {
        "generated": generated,
        "missing_run_dirs": missing_run_dirs,
        "errors": errors,
        "route_curve_artifact_gap_by_route": by_route,
    }


def _extract_trials_from_manifest(
    *,
    manifest_path: Path,
    out_csv: Path,
    min_duration_s: float,
) -> dict[str, Any]:
    trials: list[dict[str, Any]] = []
    inputs: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for run, run_dir in _run_dirs_from_manifest(manifest_path):
        if not run_dir.exists():
            continue
        paths = discover_timeseries_paths(run_dir)
        if not paths:
            inputs.append({"run_id": run.run_id, "run_dir": str(run_dir), "status": "missing_timeseries"})
            continue
        for path in paths:
            try:
                rows = load_timeseries_rows(path)
                extracted = extract_control_actuation_trials(
                    rows,
                    route_id=run.route_id,
                    backend=run.backend,
                    min_duration_s=min_duration_s,
                )
            except Exception as exc:  # noqa: BLE001 - one malformed run must not hide other evidence.
                errors.append({"run_id": run.run_id, "path": str(path), "error": str(exc)})
                continue
            trials.extend(extracted)
            inputs.append(
                {
                    "run_id": run.run_id,
                    "path": str(path),
                    "rows": len(rows),
                    "trials": len(extracted),
                }
            )
            break
    write_calibration_trials_csv(out_csv, trials)
    command_counts: dict[str, int] = {}
    for trial in trials:
        key = str(trial.get("command_type") or "unknown")
        command_counts[key] = command_counts.get(key, 0) + 1
    return {
        "path": str(out_csv),
        "trial_count": len(trials),
        "command_counts": dict(sorted(command_counts.items())),
        "inputs": inputs,
        "errors": errors,
    }


def _maybe_curve_pair(
    *,
    route_health_by_route: Mapping[str, Sequence[str]],
    out_dir: Path,
) -> dict[str, Any]:
    paths: list[Path] = []
    for route_id in ("curve217", "curve213"):
        candidates = route_health_by_route.get(route_id) or []
        if candidates:
            paths.append(Path(candidates[-1]))
    if len(paths) < 2:
        return {
            "status": "skipped",
            "reason": "curve217 and curve213 route_health reports are both required",
            "inputs": [str(path) for path in paths],
        }
    report = compare_curve_pair_files(paths)
    curve_dir = out_dir / "curve_pair"
    curve_dir.mkdir(parents=True, exist_ok=True)
    json_path = curve_dir / "curve_pair_semantics.json"
    md_path = curve_dir / "curve_pair_semantics.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_curve_pair_summary(report), encoding="utf-8")
    return {
        "status": report.get("status"),
        "inputs": [str(path) for path in paths],
        "outputs": {
            "curve_pair_semantics_json": str(json_path),
            "curve_pair_semantics_md": str(md_path),
        },
    }


def render_town01_postprocess_summary(result: Mapping[str, Any]) -> str:
    calibration = result.get("calibration") or {}
    audit = result.get("audit") or {}
    route_health = result.get("route_health") or {}
    route_curve_gap = result.get("route_curve_artifact_gap") or {}
    curve_pair = result.get("curve_pair") or {}
    lines = [
        "# Town01 Evidence Postprocess",
        "",
        f"- schema_version: `{result.get('schema_version')}`",
        f"- status: `{result.get('status')}`",
        f"- ab_reports: `{len(result.get('ab_reports') or [])}`",
        f"- route_health_existing: `{len(route_health.get('existing') or [])}`",
        f"- route_health_generated: `{len(route_health.get('generated') or [])}`",
        f"- route_curve_artifact_gap_generated: `{len(route_curve_gap.get('generated') or [])}`",
        f"- calibration_trials: `{(calibration.get('trials') or {}).get('trial_count')}`",
        f"- calibration_report: `{(calibration.get('report') or {}).get('status')}`",
        f"- curve_pair: `{curve_pair.get('status')}`",
        f"- goal_audit: `{audit.get('status')}`",
        "",
        "## Guardrails",
        "",
        "- This postprocess does not start CARLA or Apollo.",
        "- It does not make `carla_direct` the default backend.",
        "- It does not claim curve health from short-window transport evidence.",
        "- It does not modify `steer_scale=0.25` or enable physical mapping.",
        "",
    ]
    return "\n".join(lines)


def postprocess_town01_goal(
    *,
    out_dir: str | Path,
    hard_gate_batch: str | Path | None = None,
    curve_batch: str | Path | None = None,
    random_batch: str | Path | None = None,
    calibration_profile: str | Path = "configs/calibration/control_actuation.yaml",
    natural_driving_report: str | Path | None = None,
    demo_recording: str | Path | None = None,
    refresh_route_health: bool = False,
    min_trial_duration_s: float = 0.1,
    cadence_ratio_min: float = 0.8,
) -> dict[str, Any]:
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)
    batches = [
        ("hard_gates", hard_gate_batch),
        ("curves", curve_batch),
        ("random_regression", random_batch),
    ]
    ab_reports: list[dict[str, Any]] = []
    ab_report_paths: list[str] = []
    route_health_summary = {
        "generated": [],
        "existing": [],
        "missing_run_dirs": [],
        "errors": [],
        "route_health_by_route": {},
    }
    route_curve_artifact_gap_summary = {
        "generated": [],
        "missing_run_dirs": [],
        "errors": [],
        "route_curve_artifact_gap_by_route": {},
    }
    trial_manifest: Path | None = None
    for label, batch in batches:
        if batch is None:
            continue
        batch_root = Path(batch).expanduser()
        analyzed = _analyze_batch(label=label, batch_root=batch_root, out_dir=output)
        ab_reports.append(
            {
                "label": label,
                "batch_root": analyzed["batch_root"],
                "manifest_path": analyzed["manifest_path"],
                "ab_report_json": analyzed["outputs"]["ab_report_json"],
                "verdict": (analyzed["report"].get("verdict") or {}).get("status"),
            }
        )
        ab_report_paths.append(analyzed["outputs"]["ab_report_json"])
        route_health = _ensure_route_health(
            manifest_path=Path(analyzed["manifest_path"]),
            refresh=refresh_route_health,
            generated_root=output / "route_health" / label,
        )
        for key in ("generated", "existing", "missing_run_dirs", "errors"):
            route_health_summary[key].extend(route_health[key])
        for route_id, paths in route_health["route_health_by_route"].items():
            route_health_summary["route_health_by_route"].setdefault(route_id, []).extend(paths)
        route_curve_gap = _ensure_route_curve_artifact_gap(
            manifest_path=Path(analyzed["manifest_path"]),
            generated_root=output / "route_curve_artifact_gap" / label,
        )
        for key in ("generated", "missing_run_dirs", "errors"):
            route_curve_artifact_gap_summary[key].extend(route_curve_gap[key])
        for route_id, paths in route_curve_gap["route_curve_artifact_gap_by_route"].items():
            route_curve_artifact_gap_summary["route_curve_artifact_gap_by_route"].setdefault(route_id, []).extend(paths)
        if label == "hard_gates":
            trial_manifest = Path(analyzed["manifest_path"])

    curve_pair = _maybe_curve_pair(
        route_health_by_route=route_health_summary["route_health_by_route"],
        out_dir=output,
    )

    calibration_dir = output / "calibration"
    calibration_dir.mkdir(parents=True, exist_ok=True)
    trials_csv = calibration_dir / "calibration_trials.csv"
    if trial_manifest is not None:
        trials = _extract_trials_from_manifest(
            manifest_path=trial_manifest,
            out_csv=trials_csv,
            min_duration_s=min_trial_duration_s,
        )
    else:
        write_calibration_trials_csv(trials_csv, [])
        trials = {"path": str(trials_csv), "trial_count": 0, "command_counts": {}, "inputs": [], "errors": []}

    calibration_report_info: dict[str, Any] = {"status": "skipped", "reason": "missing hard-gate A/B report"}
    gate_results_path = calibration_dir / "calibration_gate_results.json"
    if ab_report_paths:
        hard_report = _read_json(Path(ab_report_paths[0]))
        gate_results = build_gate_results_from_ab_report(hard_report)
        write_gate_results(gate_results_path, gate_results)
        try:
            report, outputs = analyze_calibration_report(
                profile_path=calibration_profile,
                trials_path=trials_csv,
                gate_results_path=gate_results_path,
                out_dir=calibration_dir,
            )
            calibration_report_info = {
                "status": "written",
                "outputs": outputs,
                "promotion_allowed": (report.get("no_regression") or {}).get("promotion_allowed"),
                "missing_fields": report.get("missing_fields") or [],
                "warnings": report.get("warnings") or [],
            }
        except Exception as exc:  # noqa: BLE001 - keep postprocess output even when profile/report is missing.
            calibration_report_info = {"status": "failed", "error": str(exc)}

    audit_dir = output / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit = build_goal_audit(
        ab_report_paths=ab_report_paths or None,
        calibration_report_path=calibration_dir / "calibration_report.json",
        natural_driving_report_path=natural_driving_report,
        demo_recording_path=demo_recording,
        cadence_ratio_min=cadence_ratio_min,
    )
    audit_json = audit_dir / "town01_goal_audit.json"
    audit_md = audit_dir / "town01_goal_audit.md"
    audit_json.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    audit_md.write_text(render_goal_audit_markdown(audit), encoding="utf-8")

    result = {
        "schema_version": TOWN01_POSTPROCESS_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "status": audit.get("status"),
        "inputs": {
            "hard_gate_batch": None if hard_gate_batch is None else str(hard_gate_batch),
            "curve_batch": None if curve_batch is None else str(curve_batch),
            "random_batch": None if random_batch is None else str(random_batch),
            "calibration_profile": str(calibration_profile),
            "natural_driving_report": None if natural_driving_report is None else str(natural_driving_report),
            "demo_recording": None if demo_recording is None else str(demo_recording),
        },
        "ab_reports": ab_reports,
        "route_health": route_health_summary,
        "route_curve_artifact_gap": route_curve_artifact_gap_summary,
        "curve_pair": curve_pair,
        "calibration": {
            "trials": trials,
            "gate_results_path": str(gate_results_path) if gate_results_path.exists() else None,
            "report": calibration_report_info,
        },
        "natural_driving": (audit.get("sections") or {}).get("natural_driving") or {},
        "audit": {
            "status": audit.get("status"),
            "outputs": {
                "town01_goal_audit_json": str(audit_json),
                "town01_goal_audit_md": str(audit_md),
            },
            "missing_evidence": audit.get("missing_evidence") or [],
            "next_actions": audit.get("next_actions") or [],
            "next_action_commands": audit.get("next_action_commands") or {},
        },
    }
    result_json = output / "town01_postprocess.json"
    result_md = output / "town01_postprocess.md"
    result_json.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    result_md.write_text(render_town01_postprocess_summary(result), encoding="utf-8")
    result["outputs"] = {
        "town01_postprocess_json": str(result_json),
        "town01_postprocess_md": str(result_md),
    }
    result_json.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return result
