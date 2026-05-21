#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import default_corpus_path, load_route_corpus
from tools.analyze_town01_capability_progress import _load_csv_rows, _load_historical_summary_rows
from tools.analyze_town01_capability_review_manifest import (
    build_manifest_review_rows,
    render_report as render_manifest_report,
    write_summary_csv,
)
from tools.run_town01_capability_missing_history_packs import (
    build_missing_history_entries,
    render_runbook as render_missing_history_runbook,
    write_summary_csv as write_missing_history_summary_csv,
)
from tools.run_town01_route_health import (
    CAPABILITY_PROFILE_DEFAULT_SUBSETS,
    CAPABILITY_PROFILE_OVERRIDE_PRESET_FILES,
    _default_capability_profile_overrides,
)

CAPABILITY_PROFILES = (
    "lane_keep",
    "curve_lane_follow",
    "junction_traverse",
    "traffic_light_actual",
)

PROFILE_TO_SUBSET_PREFIX = {
    "lane_keep": "lane_keep",
    "curve_lane_follow": "curve_lane_follow",
    "junction_traverse": "junction_traverse",
    "traffic_light_actual": "traffic_light",
}

PACK_KIND_SUFFIX = {
    "focus_pack": "focus_pack",
    "proxy_pack": "proxy_pack",
    "seed_pack": "seed_pack",
    "review_priority": "review_priority_queue",
    "review_pack": "review_pack",
    "first_wave": "first_wave_smoke",
    "history_gap": "history_gap_queue",
}

CONDA_RUN_PREFIX = (
    "/home/ubuntu/miniconda3/bin/conda",
    "run",
    "-n",
    "carla16",
    "python",
)


def _subset_for_pack(capability_profile: str, pack_kind: str) -> str:
    if pack_kind == "default":
        return CAPABILITY_PROFILE_DEFAULT_SUBSETS[capability_profile]
    prefix = PROFILE_TO_SUBSET_PREFIX[capability_profile]
    suffix = PACK_KIND_SUFFIX[pack_kind]
    return f"{prefix}_{suffix}"


def _command_argv(
    *,
    capability_profile: str,
    sample_size: int,
    ticks: int,
    batch_root: Path,
    comparison_label: str,
    extra_args: Sequence[str],
    subset_name: str = "",
    route_ids_file: Path | None = None,
    overrides_file: Path | None = None,
) -> List[str]:
    argv = [
        *CONDA_RUN_PREFIX,
        str(REPO_ROOT / "tools" / "run_town01_route_health.py"),
        "run",
        "--capability-profile",
        capability_profile,
        "--ticks",
        str(ticks),
        "--batch-root",
        str(batch_root),
        "--comparison-label",
        comparison_label,
    ]
    if route_ids_file is not None:
        argv.extend(["--route-ids-file", str(route_ids_file)])
    elif subset_name:
        argv.extend(["--recommended-subset", subset_name, "--sample-size", str(sample_size)])
    if overrides_file is not None:
        argv.extend(["--overrides-file", str(overrides_file)])
    argv.extend(extra_args)
    return argv


def build_capability_review_plan(
    *,
    corpus: Dict[str, Any],
    capability_profiles: Sequence[str],
    pack_kind: str,
    sample_size: int,
    ticks: int,
    batch_root_parent: Path,
    extra_args: Sequence[str] = (),
) -> List[Dict[str, Any]]:
    subsets = dict(corpus.get("recommended_subsets") or {})
    entries: List[Dict[str, Any]] = []
    for capability_profile in capability_profiles:
        subset_name = _subset_for_pack(capability_profile, pack_kind)
        route_ids = list(subsets.get(subset_name) or [])
        comparison_label = f"{capability_profile}__{subset_name}"
        batch_root = batch_root_parent / comparison_label
        argv = _command_argv(
            capability_profile=capability_profile,
            sample_size=sample_size,
            ticks=ticks,
            batch_root=batch_root,
            comparison_label=comparison_label,
            extra_args=extra_args,
            subset_name=subset_name,
        )
        entries.append(
            {
                "capability_profile": capability_profile,
                "subset_name": subset_name,
                "route_ids": route_ids,
                "route_count": len(route_ids),
                "sample_size": sample_size,
                "ticks": ticks,
                "batch_root": str(batch_root),
                "comparison_label": comparison_label,
                "extra_args": list(extra_args),
                "subset_command_argv": argv,
                "subset_command": " ".join(shlex.quote(item) for item in argv),
                "command_argv": argv,
                "command": " ".join(shlex.quote(item) for item in argv),
            }
        )
    return entries


def freeze_capability_review_plan(
    entries: Sequence[Dict[str, Any]],
    *,
    output_root: Path,
    overrides_root: Path,
) -> List[Dict[str, Any]]:
    frozen_entries: List[Dict[str, Any]] = []
    output_root.mkdir(parents=True, exist_ok=True)
    overrides_root.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        frozen = dict(entry)
        route_ids = list(entry.get("route_ids") or [])
        route_ids_file = output_root / f"{entry['comparison_label']}.route_ids.json"
        route_ids_file.write_text(json.dumps({"route_ids": route_ids}, indent=2) + "\n", encoding="utf-8")
        capability_profile = str(entry.get("capability_profile") or "").strip()
        effective_overrides = list(_default_capability_profile_overrides(capability_profile))
        preset_overrides_file = CAPABILITY_PROFILE_OVERRIDE_PRESET_FILES.get(capability_profile)
        overrides_file = overrides_root / f"{entry['comparison_label']}.overrides.txt"
        overrides_file.write_text("".join(f"{item}\n" for item in effective_overrides), encoding="utf-8")
        argv = _command_argv(
            capability_profile=str(entry["capability_profile"]),
            sample_size=int(entry["sample_size"]),
            ticks=int(entry["ticks"]),
            batch_root=Path(str(entry["batch_root"])),
            comparison_label=str(entry["comparison_label"]),
            extra_args=list(entry.get("extra_args") or []),
            route_ids_file=route_ids_file,
            overrides_file=overrides_file,
        )
        subset_argv = _command_argv(
            capability_profile=str(entry["capability_profile"]),
            sample_size=int(entry["sample_size"]),
            ticks=int(entry["ticks"]),
            batch_root=Path(str(entry["batch_root"])),
            comparison_label=str(entry["comparison_label"]),
            extra_args=list(entry.get("extra_args") or []),
            subset_name=str(entry["subset_name"]),
            overrides_file=overrides_file,
        )
        frozen["route_ids_file"] = str(route_ids_file)
        frozen["overrides_file"] = str(overrides_file)
        frozen["preset_overrides_file"] = str(preset_overrides_file) if preset_overrides_file else ""
        frozen["effective_overrides"] = effective_overrides
        frozen["effective_sample_size"] = min(len(route_ids), int(entry.get("sample_size") or 0))
        frozen["subset_command_argv"] = subset_argv
        frozen["subset_command"] = " ".join(shlex.quote(item) for item in subset_argv)
        frozen["frozen_command_argv"] = argv
        frozen["frozen_command"] = " ".join(shlex.quote(item) for item in argv)
        frozen["command_argv"] = argv
        frozen["command"] = frozen["frozen_command"]
        frozen_entries.append(frozen)
    return frozen_entries


def render_runbook(
    *,
    entries: Sequence[Dict[str, Any]],
    corpus_path: Path,
    pack_kind: str,
    sample_size: int,
    ticks: int,
) -> str:
    lines = [
        "# Town01 Capability Review Runbook",
        "",
        f"- generated_at_local: `{datetime.now().isoformat(timespec='seconds')}`",
        f"- corpus_path: `{corpus_path}`",
        f"- pack_kind: `{pack_kind}`",
        f"- sample_size: `{sample_size}`",
        f"- ticks: `{ticks}`",
        "",
        "## Summary",
        "",
    ]
    for entry in entries:
        route_ids = entry["route_ids"]
        lines.append(
            f"- `{entry['capability_profile']}` -> `{entry['subset_name']}` | routes=`{len(route_ids)}` | route_ids=`{', '.join(route_ids) or 'none'}`"
        )
    lines.extend(["", "## Commands", ""])
    for entry in entries:
        lines.append(f"### {entry['capability_profile']}")
        lines.append("")
        lines.append(f"- subset: `{entry['subset_name']}`")
        lines.append(f"- batch_root: `{entry['batch_root']}`")
        lines.append(f"- comparison_label: `{entry['comparison_label']}`")
        lines.append(f"- route_ids: `{', '.join(entry['route_ids']) or 'none'}`")
        if entry.get("route_ids_file"):
            lines.append(f"- route_ids_file: `{entry['route_ids_file']}`")
        if entry.get("preset_overrides_file"):
            lines.append(f"- preset_overrides_file: `{entry['preset_overrides_file']}`")
        if entry.get("overrides_file"):
            lines.append(f"- overrides_file: `{entry['overrides_file']}`")
        if entry.get("effective_sample_size") is not None:
            lines.append(f"- effective_sample_size: `{entry['effective_sample_size']}`")
        if entry.get("effective_overrides"):
            lines.append(f"- effective_overrides: `{', '.join(entry['effective_overrides'])}`")
        lines.append("")
        if entry.get("frozen_command"):
            lines.append("```bash")
            lines.append(str(entry["frozen_command"]))
            lines.append("```")
            lines.append("")
        lines.append("```bash")
        lines.append(str(entry.get("subset_command") or entry["command"]))
        lines.append("```")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build or execute standardized Town01 capability review packs.")
    parser.add_argument(
        "--capability",
        action="append",
        choices=CAPABILITY_PROFILES,
        default=[],
        help="Capability profile(s) to include. Defaults to all.",
    )
    parser.add_argument(
        "--pack-kind",
        choices=("default", "focus_pack", "proxy_pack", "seed_pack", "review_priority", "review_pack", "first_wave", "history_gap"),
        default="default",
    )
    parser.add_argument("--corpus", type=Path, default=default_corpus_path(REPO_ROOT))
    parser.add_argument("--sample-size", type=int, default=4)
    parser.add_argument("--ticks", type=int, default=700)
    parser.add_argument("--batch-root-parent", type=Path, default=REPO_ROOT / "runs" / f"town01_capability_review_{datetime.now().strftime('%Y%m%d')}")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--manifest-output", type=Path, default=None)
    parser.add_argument("--review-report-output", type=Path, default=None)
    parser.add_argument("--review-summary-output", type=Path, default=None)
    parser.add_argument("--missing-history-runbook-output", type=Path, default=None)
    parser.add_argument("--missing-history-manifest-output", type=Path, default=None)
    parser.add_argument("--missing-history-summary-output", type=Path, default=None)
    parser.add_argument("--extra-arg", action="append", default=[], help="Extra arg(s) appended to each underlying run command.")
    parser.add_argument("--run", action="store_true", help="Execute the generated commands sequentially.")
    parser.add_argument("--continue-on-failure", action="store_true")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    capability_profiles = list(args.capability or CAPABILITY_PROFILES)
    corpus_path = Path(args.corpus).expanduser().resolve()
    batch_root_parent = Path(args.batch_root_parent).expanduser().resolve()
    stamp = datetime.now().strftime("%Y%m%d")
    batch_artifacts_root = batch_root_parent / "artifacts"
    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output is not None
        else (batch_artifacts_root / f"town01_capability_review_runbook_{stamp}.md").resolve()
    )
    manifest_output_path = (
        Path(args.manifest_output).expanduser().resolve()
        if args.manifest_output is not None
        else (batch_artifacts_root / f"town01_capability_review_manifest_{stamp}.json").resolve()
    )
    review_report_output_path = (
        Path(args.review_report_output).expanduser().resolve()
        if args.review_report_output is not None
        else (batch_artifacts_root / f"town01_capability_review_manifest_report_{stamp}.md").resolve()
    )
    review_summary_output_path = (
        Path(args.review_summary_output).expanduser().resolve()
        if args.review_summary_output is not None
        else (batch_artifacts_root / f"town01_capability_review_manifest_summary_{stamp}.csv").resolve()
    )
    missing_history_runbook_output_path = (
        Path(args.missing_history_runbook_output).expanduser().resolve()
        if args.missing_history_runbook_output is not None
        else (batch_artifacts_root / f"town01_capability_missing_history_runbook_{stamp}.md").resolve()
    )
    missing_history_manifest_output_path = (
        Path(args.missing_history_manifest_output).expanduser().resolve()
        if args.missing_history_manifest_output is not None
        else (batch_artifacts_root / f"town01_capability_missing_history_manifest_{stamp}.json").resolve()
    )
    missing_history_summary_output_path = (
        Path(args.missing_history_summary_output).expanduser().resolve()
        if args.missing_history_summary_output is not None
        else (batch_artifacts_root / f"town01_capability_missing_history_summary_{stamp}.csv").resolve()
    )
    corpus = load_route_corpus(corpus_path)
    entries = build_capability_review_plan(
        corpus=corpus,
        capability_profiles=capability_profiles,
        pack_kind=str(args.pack_kind),
        sample_size=int(args.sample_size),
        ticks=int(args.ticks),
        batch_root_parent=batch_root_parent,
        extra_args=list(args.extra_arg or []),
    )
    frozen_route_ids_root = output_path.parent / f"{output_path.stem}_route_ids"
    frozen_overrides_root = output_path.parent / f"{output_path.stem}_overrides"
    entries = freeze_capability_review_plan(entries, output_root=frozen_route_ids_root, overrides_root=frozen_overrides_root)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_runbook(
            entries=entries,
            corpus_path=corpus_path,
            pack_kind=str(args.pack_kind),
            sample_size=int(args.sample_size),
            ticks=int(args.ticks),
        ),
        encoding="utf-8",
    )
    manifest_output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_output_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "corpus_path": str(corpus_path),
                "pack_kind": str(args.pack_kind),
                "sample_size": int(args.sample_size),
                "ticks": int(args.ticks),
                "entries": entries,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    comparison_path = REPO_ROOT / "artifacts" / "town01_route_health_platform_comparison.csv"
    comparison_rows = _load_csv_rows(comparison_path)
    historical_summary_rows = _load_historical_summary_rows(REPO_ROOT / "runs")
    review_rows = build_manifest_review_rows(
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "corpus_path": str(corpus_path),
            "pack_kind": str(args.pack_kind),
            "sample_size": int(args.sample_size),
            "ticks": int(args.ticks),
            "entries": entries,
        },
        corpus,
        comparison_rows,
        historical_summary_rows,
    )
    review_report_output_path.parent.mkdir(parents=True, exist_ok=True)
    review_report_output_path.write_text(
        render_manifest_report(
            review_rows,
            manifest_path=manifest_output_path,
            comparison_path=comparison_path,
            corpus_path=corpus_path,
        ),
        encoding="utf-8",
    )
    write_summary_csv(review_summary_output_path, review_rows)
    missing_history_route_ids_root = missing_history_runbook_output_path.parent / f"{missing_history_runbook_output_path.stem}_route_ids"
    missing_history_overrides_root = missing_history_runbook_output_path.parent / f"{missing_history_runbook_output_path.stem}_overrides"
    missing_history_batch_root_parent = REPO_ROOT / "runs" / f"{missing_history_runbook_output_path.stem}"
    missing_history_entries = build_missing_history_entries(
        review_rows,
        route_ids_root=missing_history_route_ids_root,
        overrides_root=missing_history_overrides_root,
        batch_root_parent=missing_history_batch_root_parent,
    )
    missing_history_manifest_output_path.parent.mkdir(parents=True, exist_ok=True)
    missing_history_manifest_output_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "source_manifest": str(manifest_output_path),
                "source_review_report": str(review_report_output_path),
                "entries": missing_history_entries,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    missing_history_runbook_output_path.parent.mkdir(parents=True, exist_ok=True)
    missing_history_runbook_output_path.write_text(
        render_missing_history_runbook(
            missing_history_entries,
            source_manifest=manifest_output_path,
            source_report=review_report_output_path,
        ),
        encoding="utf-8",
    )
    write_missing_history_summary_csv(missing_history_summary_output_path, missing_history_entries)
    print(f"[town01-capability-review-packs] written: {output_path}")
    print(f"[town01-capability-review-packs] manifest: {manifest_output_path}")
    print(f"[town01-capability-review-packs] review-report: {review_report_output_path}")
    print(f"[town01-capability-review-packs] review-summary: {review_summary_output_path}")
    print(f"[town01-capability-review-packs] missing-history-runbook: {missing_history_runbook_output_path}")
    print(f"[town01-capability-review-packs] missing-history-manifest: {missing_history_manifest_output_path}")
    print(f"[town01-capability-review-packs] missing-history-summary: {missing_history_summary_output_path}")

    if args.run:
        for entry in entries:
            proc = subprocess.run(
                list(entry["command_argv"]),
                cwd=str(REPO_ROOT),
                check=False,
            )
            if proc.returncode != 0 and not bool(args.continue_on_failure):
                return proc.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
