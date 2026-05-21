#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
import csv
import json
from pathlib import Path
import shlex
import sys
from typing import Any, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import default_corpus_path, load_route_corpus
from tools.analyze_town01_capability_progress import _load_csv_rows, _load_historical_summary_rows
from tools.analyze_town01_capability_review_manifest import build_manifest_review_rows
from tools.run_town01_route_health import _load_overrides_file

CONDA_RUN_PREFIX = (
    "/home/ubuntu/miniconda3/bin/conda",
    "run",
    "-n",
    "carla16",
    "python",
)


def _write_route_ids_file(path: Path, route_ids: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"route_ids": list(route_ids)}, indent=2) + "\n", encoding="utf-8")


def _write_overrides_file(path: Path, overrides: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{item}\n" for item in list(overrides)), encoding="utf-8")


def _command_argv(
    *,
    capability_profile: str,
    route_ids_file: Path,
    overrides_file: Path,
    ticks: int,
    batch_root: Path,
    comparison_label: str,
    extra_args: Sequence[str],
) -> List[str]:
    argv = [
        *CONDA_RUN_PREFIX,
        str(REPO_ROOT / "tools" / "run_town01_route_health.py"),
        "run",
        "--capability-profile",
        capability_profile,
        "--route-ids-file",
        str(route_ids_file),
        "--overrides-file",
        str(overrides_file),
        "--ticks",
        str(ticks),
        "--batch-root",
        str(batch_root),
        "--comparison-label",
        comparison_label,
    ]
    argv.extend(extra_args)
    return argv


def build_missing_history_entries(
    review_rows: Sequence[Dict[str, Any]],
    *,
    route_ids_root: Path,
    overrides_root: Path,
    batch_root_parent: Path,
) -> List[Dict[str, Any]]:
    route_ids_root.mkdir(parents=True, exist_ok=True)
    overrides_root.mkdir(parents=True, exist_ok=True)
    entries: List[Dict[str, Any]] = []
    for row in review_rows:
        missing_history_routes = [
            str(route_id).strip()
            for route_id in list(row.get("missing_history_routes") or [])
            if str(route_id).strip()
        ]
        if not missing_history_routes:
            continue
        capability_profile = str(row.get("capability_profile") or "").strip()
        source_subset_name = str(row.get("subset_name") or "").strip()
        source_comparison_label = str(row.get("comparison_label") or "").strip()
        comparison_label = f"{source_comparison_label}__missing_history"
        route_ids_file = route_ids_root / f"{comparison_label}.route_ids.json"
        _write_route_ids_file(route_ids_file, missing_history_routes)
        seed_route_ids = list(missing_history_routes[:1])
        seed_comparison_label = f"{comparison_label}__seed"
        seed_route_ids_file = route_ids_root / f"{seed_comparison_label}.route_ids.json"
        _write_route_ids_file(seed_route_ids_file, seed_route_ids)
        source_overrides_file = Path(str(row.get("overrides_file") or "")).expanduser()
        effective_overrides = (
            _load_overrides_file(source_overrides_file)
            if source_overrides_file.exists()
            else []
        )
        overrides_file = overrides_root / f"{comparison_label}.overrides.txt"
        _write_overrides_file(overrides_file, effective_overrides)
        batch_root = batch_root_parent / comparison_label
        seed_batch_root = batch_root_parent / seed_comparison_label
        ticks = int(row.get("ticks") or 700)
        extra_args = [str(item) for item in list(row.get("extra_args") or []) if str(item).strip()]
        argv = _command_argv(
            capability_profile=capability_profile,
            route_ids_file=route_ids_file,
            overrides_file=overrides_file,
            ticks=ticks,
            batch_root=batch_root,
            comparison_label=comparison_label,
            extra_args=extra_args,
        )
        seed_argv = _command_argv(
            capability_profile=capability_profile,
            route_ids_file=seed_route_ids_file,
            overrides_file=overrides_file,
            ticks=ticks,
            batch_root=seed_batch_root,
            comparison_label=seed_comparison_label,
            extra_args=extra_args,
        )
        entries.append(
            {
                "capability_profile": capability_profile,
                "source_subset_name": source_subset_name,
                "source_comparison_label": source_comparison_label,
                "source_pack_readiness": str(row.get("pack_readiness") or "").strip(),
                "missing_history_routes": missing_history_routes,
                "seed_route_id": seed_route_ids[0] if seed_route_ids else "",
                "seed_route_ids_file": str(seed_route_ids_file),
                "seed_batch_root": str(seed_batch_root),
                "seed_comparison_label": seed_comparison_label,
                "route_count": len(missing_history_routes),
                "route_ids_file": str(route_ids_file),
                "preset_overrides_file": str(row.get("preset_overrides_file") or "").strip(),
                "source_overrides_file": str(source_overrides_file),
                "overrides_file": str(overrides_file),
                "effective_overrides": effective_overrides,
                "ticks": ticks,
                "batch_root": str(batch_root),
                "comparison_label": comparison_label,
                "extra_args": extra_args,
                "seed_command_argv": seed_argv,
                "seed_command": " ".join(shlex.quote(item) for item in seed_argv),
                "command_argv": argv,
                "command": " ".join(shlex.quote(item) for item in argv),
            }
        )
    return entries


def render_runbook(
    entries: Sequence[Dict[str, Any]],
    *,
    source_manifest: Path,
    source_report: Path,
) -> str:
    lines = [
        "# Town01 Capability Missing-History Runbook",
        "",
        f"- generated_at_local: `{datetime.now().isoformat(timespec='seconds')}`",
        f"- source_manifest: `{source_manifest}`",
        f"- source_report: `{source_report}`",
        "",
        "## Summary",
        "",
    ]
    for entry in entries:
        lines.append(
            f"- `{entry['capability_profile']}` <- `{entry['source_subset_name']}` | missing_history_routes=`{len(entry['missing_history_routes'])}` | seed_route=`{entry.get('seed_route_id') or 'none'}` | source_readiness=`{entry['source_pack_readiness']}` | route_ids=`{', '.join(entry['missing_history_routes'])}`"
        )
    lines.extend(["", "## Commands", ""])
    for entry in entries:
        lines.append(f"### {entry['capability_profile']}")
        lines.append("")
        lines.append(f"- source_subset: `{entry['source_subset_name']}`")
        lines.append(f"- source_comparison_label: `{entry['source_comparison_label']}`")
        lines.append(f"- source_pack_readiness: `{entry['source_pack_readiness']}`")
        lines.append(f"- seed_route_id: `{entry.get('seed_route_id') or 'none'}`")
        lines.append(f"- seed_route_ids_file: `{entry.get('seed_route_ids_file') or 'none'}`")
        lines.append(f"- seed_batch_root: `{entry.get('seed_batch_root') or 'none'}`")
        lines.append(f"- route_ids: `{', '.join(entry['missing_history_routes'])}`")
        lines.append(f"- route_ids_file: `{entry['route_ids_file']}`")
        lines.append(f"- preset_overrides_file: `{entry['preset_overrides_file'] or 'none'}`")
        lines.append(f"- source_overrides_file: `{entry['source_overrides_file'] or 'none'}`")
        lines.append(f"- overrides_file: `{entry['overrides_file']}`")
        lines.append(f"- batch_root: `{entry['batch_root']}`")
        if entry.get("effective_overrides"):
            lines.append(f"- effective_overrides: `{', '.join(entry['effective_overrides'])}`")
        lines.append("")
        if entry.get("seed_command"):
            lines.append("```bash")
            lines.append(str(entry["seed_command"]))
            lines.append("```")
            lines.append("")
        lines.append("```bash")
        lines.append(str(entry["command"]))
        lines.append("```")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_summary_csv(path: Path, entries: Sequence[Dict[str, Any]]) -> None:
    fieldnames = [
        "capability_profile",
        "source_subset_name",
        "source_comparison_label",
        "source_pack_readiness",
        "seed_route_id",
        "seed_route_ids_file",
        "seed_batch_root",
        "seed_comparison_label",
        "route_count",
        "missing_history_routes",
        "route_ids_file",
        "preset_overrides_file",
        "source_overrides_file",
        "overrides_file",
        "batch_root",
        "comparison_label",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for entry in entries:
            writer.writerow(
                {
                    "capability_profile": entry["capability_profile"],
                    "source_subset_name": entry["source_subset_name"],
                    "source_comparison_label": entry["source_comparison_label"],
                    "source_pack_readiness": entry["source_pack_readiness"],
                    "seed_route_id": entry["seed_route_id"],
                    "seed_route_ids_file": entry["seed_route_ids_file"],
                    "seed_batch_root": entry["seed_batch_root"],
                    "seed_comparison_label": entry["seed_comparison_label"],
                    "route_count": entry["route_count"],
                    "missing_history_routes": ",".join(entry["missing_history_routes"]),
                    "route_ids_file": entry["route_ids_file"],
                    "preset_overrides_file": entry["preset_overrides_file"],
                    "source_overrides_file": entry["source_overrides_file"],
                    "overrides_file": entry["overrides_file"],
                    "batch_root": entry["batch_root"],
                    "comparison_label": entry["comparison_label"],
                }
            )


def _build_parser() -> argparse.ArgumentParser:
    stamp = datetime.now().strftime("%Y%m%d")
    parser = argparse.ArgumentParser(description="Freeze Town01 capability missing-history rerun packs.")
    parser.add_argument("--manifest", type=Path, default=REPO_ROOT / "artifacts" / f"town01_capability_review_manifest_{stamp}.json")
    parser.add_argument("--source-report", type=Path, default=REPO_ROOT / "artifacts" / f"town01_capability_review_manifest_report_{stamp}.md")
    parser.add_argument("--corpus", type=Path, default=default_corpus_path(REPO_ROOT))
    parser.add_argument("--comparison", type=Path, default=REPO_ROOT / "artifacts" / "town01_route_health_platform_comparison.csv")
    parser.add_argument("--runs-root", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--manifest-output", type=Path, default=None)
    parser.add_argument("--summary-csv-output", type=Path, default=None)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    manifest_path = Path(args.manifest).expanduser().resolve()
    source_report_path = Path(args.source_report).expanduser().resolve()
    corpus_path = Path(args.corpus).expanduser().resolve()
    comparison_path = Path(args.comparison).expanduser().resolve()
    runs_root = Path(args.runs_root).expanduser().resolve()
    batch_artifacts_root = manifest_path.parent
    stamp = datetime.now().strftime("%Y%m%d")
    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output is not None
        else (batch_artifacts_root / f"town01_capability_missing_history_runbook_{stamp}.md").resolve()
    )
    manifest_output_path = (
        Path(args.manifest_output).expanduser().resolve()
        if args.manifest_output is not None
        else (batch_artifacts_root / f"town01_capability_missing_history_manifest_{stamp}.json").resolve()
    )
    summary_csv_output_path = (
        Path(args.summary_csv_output).expanduser().resolve()
        if args.summary_csv_output is not None
        else (batch_artifacts_root / f"town01_capability_missing_history_summary_{stamp}.csv").resolve()
    )

    route_ids_root = output_path.parent / f"{output_path.stem}_route_ids"
    overrides_root = output_path.parent / f"{output_path.stem}_overrides"
    batch_root_parent = REPO_ROOT / "runs" / f"{output_path.stem}"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    corpus = load_route_corpus(corpus_path)
    comparison_rows = _load_csv_rows(comparison_path)
    historical_summary_rows = _load_historical_summary_rows(runs_root)
    review_rows = build_manifest_review_rows(manifest, corpus, comparison_rows, historical_summary_rows)
    entries = build_missing_history_entries(
        review_rows,
        route_ids_root=route_ids_root,
        overrides_root=overrides_root,
        batch_root_parent=batch_root_parent,
    )

    manifest_output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_output_path.write_text(
        json.dumps(
            {
                "generated_at_local": datetime.now().isoformat(timespec="seconds"),
                "source_manifest": str(manifest_path),
                "entries": entries,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_runbook(entries, source_manifest=manifest_path, source_report=source_report_path),
        encoding="utf-8",
    )
    write_summary_csv(summary_csv_output_path, entries)
    print(f"[town01-capability-missing-history] runbook: {output_path}")
    print(f"[town01-capability-missing-history] manifest: {manifest_output_path}")
    print(f"[town01-capability-missing-history] summary: {summary_csv_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
