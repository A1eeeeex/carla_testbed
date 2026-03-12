#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from evaluate_gt_baseline import evaluate_run


METRIC_KEYS = [
    "routing_success",
    "launch_success",
    "max_speed",
    "stable_follow_gap_m",
    "stop_gap_m",
    "control_command_effective_rate",
    "watchdog_trigger_count",
    "abnormal_stop_before_goal",
]


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _parse_mapping(items: Optional[List[str]]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for item in items or []:
        if "=" not in item:
            raise SystemExit(f"invalid mapping (expect key=value): {item}")
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise SystemExit(f"invalid mapping (expect non-empty key/value): {item}")
        mapping[key] = value
    return mapping


def _discover_runs(
    run_dirs: Optional[List[str]],
    runs_root: Optional[str],
    run_names: Optional[List[str]],
) -> List[Path]:
    discovered: List[Path] = []
    if run_dirs:
        for item in run_dirs:
            path = Path(item).expanduser().resolve()
            if not path.is_dir():
                raise SystemExit(f"run directory not found: {path}")
            discovered.append(path)
        return discovered

    if not runs_root:
        raise SystemExit("either --run-dir or --runs-root is required")

    root = Path(runs_root).expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"runs root not found: {root}")

    if run_names:
        for name in run_names:
            candidate = (root / name).resolve()
            if not candidate.is_dir():
                raise SystemExit(f"run not found under runs-root: {candidate}")
            discovered.append(candidate)
        return discovered

    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        if (child / "summary.json").exists() or (child / "artifacts" / "gt_baseline_metrics.json").exists():
            discovered.append(child.resolve())
    if not discovered:
        raise SystemExit(f"no run directories found under: {root}")
    return discovered


def _default_output_dir(run_dirs: List[Path], runs_root: Optional[str]) -> Path:
    if runs_root:
        return (Path(runs_root).expanduser().resolve() / "artifacts").resolve()
    if len(run_dirs) == 1:
        return (run_dirs[0] / "artifacts").resolve()
    parents = {run_dir.parent.resolve() for run_dir in run_dirs}
    if len(parents) == 1:
        parent = next(iter(parents))
        return (parent / "artifacts").resolve()
    return (Path.cwd() / "artifacts").resolve()


def _ensure_metrics(run_dir: Path, recompute: bool, no_md: bool) -> Dict[str, Any]:
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = artifacts_dir / "gt_baseline_metrics.json"

    if (not recompute) and metrics_path.exists():
        cached = _load_json(metrics_path)
        if cached is not None:
            return cached

    result = evaluate_run(run_dir)
    metrics_path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    if not no_md:
        md_path = artifacts_dir / "gt_baseline_metrics.md"
        md_lines = _render_run_metrics_markdown(result)
        md_path.write_text(md_lines)
    return result


def _render_run_metrics_markdown(result: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# GT Baseline Metrics")
    lines.append("")
    lines.append(f"- run_dir: `{result.get('run_dir', '')}`")
    lines.append(f"- generated_at_utc: `{result.get('generated_at_utc', '')}`")
    lines.append("")
    lines.append("| Metric | Value | Status | Source |")
    lines.append("|---|---:|---|---|")
    metrics = result.get("metrics", {}) or {}
    for key in METRIC_KEYS:
        payload = metrics.get(key, {}) or {}
        value = payload.get("value")
        if isinstance(value, dict):
            if "max_speed_mps" in value:
                value_text = f"{value.get('max_speed_mps')} m/s ({value.get('max_speed_kmh')} km/h)"
            else:
                value_text = json.dumps(value, ensure_ascii=False)
        else:
            value_text = "null" if value is None else str(value)
        status = str(payload.get("metric_status", ""))
        src = payload.get("source")
        if isinstance(src, dict):
            src_text = json.dumps(src, ensure_ascii=False)
        elif isinstance(src, list):
            src_text = ", ".join(str(item) for item in src)
        else:
            src_text = str(src)
        lines.append(f"| {key} | {value_text} | {status} | {src_text} |")
    return "\n".join(lines) + "\n"


def _detect_profile(
    run_dir: Path,
    profile_override: Dict[str, str],
) -> Dict[str, Any]:
    run_key = run_dir.name
    path_key = str(run_dir.resolve())
    overridden = profile_override.get(path_key) or profile_override.get(run_key)

    summary = _load_json(run_dir / "summary.json") or {}
    profile_info = _load_json(run_dir / "artifacts" / "profile_info.json") or {}

    profile_name = (
        overridden
        or str(profile_info.get("profile_name") or "").strip()
        or str(summary.get("profile_name") or "").strip()
        or run_key
    )
    profile_config_path = (
        str(profile_info.get("profile_config_path") or "").strip()
        or str(summary.get("profile_config_path") or "").strip()
    )

    source = "override" if overridden else "run_artifacts"
    if not profile_info and not summary:
        source = "fallback_run_name"
    return {
        "profile_name": profile_name,
        "profile_config_path": profile_config_path,
        "profile_source": source,
    }


def _format_metric_value(metric_name: str, payload: Dict[str, Any]) -> str:
    value = payload.get("value")
    if metric_name == "max_speed" and isinstance(value, dict):
        kmh = value.get("max_speed_kmh")
        mps = value.get("max_speed_mps")
        if kmh is None and mps is None:
            return "null"
        if kmh is None:
            return f"{mps} m/s"
        if mps is None:
            return f"{kmh} km/h"
        return f"{mps} m/s ({kmh} km/h)"
    if isinstance(value, float):
        return f"{value:.4f}"
    return "null" if value is None else str(value)


def _make_compare_record(run_dir: Path, metrics_result: Dict[str, Any], profile_info: Dict[str, Any]) -> Dict[str, Any]:
    metrics = metrics_result.get("metrics", {}) or {}
    compact: Dict[str, Any] = {}
    metric_notes: List[str] = []

    for key in METRIC_KEYS:
        payload = metrics.get(key, {}) or {}
        compact[key] = {
            "value": payload.get("value"),
            "metric_status": payload.get("metric_status"),
            "source": payload.get("source"),
            "rule": payload.get("rule"),
            "details": payload.get("details"),
        }
        status = str(payload.get("metric_status") or "")
        if status in {"not_available", "best_effort"}:
            metric_notes.append(f"{key}:{status}")

    return {
        "run_name": run_dir.name,
        "run_dir": str(run_dir.resolve()),
        "profile_name": profile_info.get("profile_name"),
        "profile_config_path": profile_info.get("profile_config_path"),
        "profile_source": profile_info.get("profile_source"),
        "metrics": compact,
        "metric_notes": metric_notes,
        "run_notes": metrics_result.get("notes", []) or [],
    }


def _render_compare_markdown(compare: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# GT Profile Compare")
    lines.append("")
    lines.append(f"- generated_at_utc: `{compare.get('generated_at_utc', '')}`")
    lines.append(f"- run_count: `{len(compare.get('runs', []))}`")
    lines.append("")
    lines.append(
        "| Profile | Run | routing_success | launch_success | max_speed | "
        "stable_follow_gap_m | stop_gap_m | control_effective_rate | watchdog_trigger_count | abnormal_stop_before_goal |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for run in compare.get("runs", []):
        metrics = run.get("metrics", {}) or {}

        def metric_text(name: str) -> str:
            payload = metrics.get(name, {}) or {}
            value_text = _format_metric_value(name, payload)
            status = str(payload.get("metric_status", ""))
            return f"{value_text} [{status}]"

        lines.append(
            "| "
            + f"{run.get('profile_name', '')} | {run.get('run_name', '')} | "
            + f"{metric_text('routing_success')} | "
            + f"{metric_text('launch_success')} | "
            + f"{metric_text('max_speed')} | "
            + f"{metric_text('stable_follow_gap_m')} | "
            + f"{metric_text('stop_gap_m')} | "
            + f"{metric_text('control_command_effective_rate')} | "
            + f"{metric_text('watchdog_trigger_count')} | "
            + f"{metric_text('abnormal_stop_before_goal')} |"
        )

    lines.append("")
    lines.append("## Run Meta")
    lines.append("")
    lines.append("| Profile | Run | Config Path | Profile Source | Notes |")
    lines.append("|---|---|---|---|---|")
    for run in compare.get("runs", []):
        notes = ", ".join(run.get("metric_notes", []) or [])
        lines.append(
            "| "
            + f"{run.get('profile_name', '')} | {run.get('run_name', '')} | "
            + f"{run.get('profile_config_path', '')} | {run.get('profile_source', '')} | {notes} |"
        )

    global_notes = compare.get("notes", []) or []
    if global_notes:
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        for note in global_notes:
            lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare GT baseline metrics across minimal/relaxed/strict runs")
    parser.add_argument("--run-dir", action="append", help="Run dir path. Repeat for multiple runs.")
    parser.add_argument("--runs-root", help="Root directory containing runs/<run_name> subdirectories.")
    parser.add_argument(
        "--run-name",
        action="append",
        help="Run name under --runs-root. Repeat to select specific runs. Default: auto-discover.",
    )
    parser.add_argument(
        "--profile-map",
        action="append",
        help="Override profile label mapping, format: <run_name_or_abs_run_dir>=<profile_name>",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory for gt_profile_compare.json/.md. Default: runs-root/artifacts or inferred artifacts dir.",
    )
    parser.add_argument("--recompute", action="store_true", help="Recompute per-run baseline metrics even if cached.")
    parser.add_argument(
        "--no-md",
        action="store_true",
        help="Skip writing markdown outputs (both per-run metrics and compare markdown).",
    )
    args = parser.parse_args()

    run_dirs = _discover_runs(args.run_dir, args.runs_root, args.run_name)
    profile_override = _parse_mapping(args.profile_map)
    out_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else _default_output_dir(run_dirs, args.runs_root)
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    records: List[Dict[str, Any]] = []
    notes: List[str] = []
    for run_dir in run_dirs:
        metrics_result = _ensure_metrics(run_dir, recompute=args.recompute, no_md=args.no_md)
        profile_info = _detect_profile(run_dir, profile_override)
        record = _make_compare_record(run_dir, metrics_result, profile_info)
        records.append(record)

    seen_profile_names = [str(item.get("profile_name", "")) for item in records]
    if len(seen_profile_names) != len(set(seen_profile_names)):
        notes.append("profile_name duplicated across runs; use --profile-map to disambiguate labels")
    if len(records) < 2:
        notes.append("only one run provided; compare table still generated but lacks cross-profile contrast")

    compare = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "metrics_index": list(METRIC_KEYS),
        "runs": records,
        "notes": notes,
    }

    out_json = out_dir / "gt_profile_compare.json"
    out_json.write_text(json.dumps(compare, indent=2, ensure_ascii=False))

    if not args.no_md:
        out_md = out_dir / "gt_profile_compare.md"
        out_md.write_text(_render_compare_markdown(compare))
        print(f"[gt-profile-compare] written: {out_md}")
    print(f"[gt-profile-compare] written: {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
