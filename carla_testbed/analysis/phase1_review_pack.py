from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
import time
from pathlib import Path
from typing import Any, Mapping

PHASE1_REVIEW_PACK_SCHEMA_VERSION = "phase1_review_pack.v1"
STATUS_DONE = "DONE"
STATUS_PARTIAL = "PARTIAL"


def build_phase1_review_pack(
    catalog_path: str | Path,
    out_dir: str | Path,
    *,
    repo_root: str | Path = ".",
) -> dict[str, Any]:
    """Materialize a self-contained Phase 1 review pack directory.

    The pack consumes an already-generated Phase 1 scenario catalog and copies
    only the exact accepted bundles referenced by each catalog row. It does not
    re-rank run evidence and does not synthesize a better status from unrelated
    runs.
    """

    catalog = Path(catalog_path).expanduser()
    repo = Path(repo_root).expanduser().resolve()
    output = Path(out_dir).expanduser()
    output.mkdir(parents=True, exist_ok=True)

    catalog_payload = _read_json(catalog)
    copied_files: list[dict[str, Any]] = []
    missing_required: list[dict[str, Any]] = []
    scenario_entries: list[dict[str, Any]] = []

    catalog_dest = output / "catalog" / catalog.name
    _copy_required(catalog, catalog_dest, "phase1_scenario_catalog_json", copied_files, missing_required)
    catalog_md = catalog.with_suffix(".md")
    if catalog_md.exists():
        _copy_required(
            catalog_md,
            output / "catalog" / catalog_md.name,
            "phase1_scenario_catalog_markdown",
            copied_files,
            missing_required,
        )

    for scenario in catalog_payload.get("scenarios") or []:
        if not isinstance(scenario, Mapping):
            continue
        scenario_name = str(scenario.get("scenario") or "unknown_scenario")
        accepted_path_raw = scenario.get("accepted_bundle_path")
        scenario_entry: dict[str, Any] = {
            "scenario": scenario_name,
            "catalog_overall_status": scenario.get("overall_status"),
            "accepted_bundle_status": scenario.get("accepted_bundle_status"),
            "accepted_bundle_path": accepted_path_raw,
            "included": False,
            "status": STATUS_PARTIAL,
            "blocking_reasons": [],
        }
        if not accepted_path_raw:
            scenario_entry["blocking_reasons"].append("accepted_bundle_path_missing")
            missing_required.append({"role": "accepted_bundle_path", "scenario": scenario_name})
            scenario_entries.append(scenario_entry)
            continue

        accepted_report = _resolve_path(str(accepted_path_raw), repo)
        if not accepted_report.exists():
            scenario_entry["blocking_reasons"].append("accepted_bundle_report_missing")
            missing_required.append(
                {
                    "role": "accepted_bundle_report",
                    "scenario": scenario_name,
                    "source": str(accepted_report),
                }
            )
            scenario_entries.append(scenario_entry)
            continue

        accepted_payload = _read_json(accepted_report)
        source_root = accepted_report.parent
        dest_root = output / "acceptance" / _safe_name(scenario_name)
        copied = _copy_tree(source_root, dest_root, copied_files)
        bundle_self_contained = _bundle_self_contained(accepted_payload)
        accepted_done = str(accepted_payload.get("status") or "") == STATUS_DONE
        if not accepted_done:
            scenario_entry["blocking_reasons"].append("accepted_bundle_not_done")
        if not bundle_self_contained:
            scenario_entry["blocking_reasons"].append("accepted_bundle_not_self_contained")
        if not copied:
            scenario_entry["blocking_reasons"].append("accepted_bundle_copy_empty")
        scenario_entry.update(
            {
                "included": bool(copied),
                "status": STATUS_DONE if accepted_done and bundle_self_contained and copied else STATUS_PARTIAL,
                "comparison_id": accepted_payload.get("comparison_id"),
                "apollo_run_id": accepted_payload.get("apollo_run_id"),
                "planning_control_run_id": accepted_payload.get("planning_control_run_id"),
                "bundle_self_contained": bundle_self_contained,
                "copied_file_count": len(copied),
                "pack_relative_path": str(dest_root.relative_to(output)),
            }
        )
        scenario_entries.append(scenario_entry)

    scenario_blockers = [
        {"scenario": item.get("scenario"), "blocking_reasons": item.get("blocking_reasons")}
        for item in scenario_entries
        if item.get("status") != STATUS_DONE or item.get("blocking_reasons")
    ]
    status = STATUS_DONE if not missing_required and not scenario_blockers else STATUS_PARTIAL
    manifest = {
        "schema_version": PHASE1_REVIEW_PACK_SCHEMA_VERSION,
        "generated_at_wall_s": time.time(),
        "status": status,
        "source_catalog_path": str(catalog),
        "catalog_summary": catalog_payload.get("summary") or {},
        "scenario_count": len(scenario_entries),
        "done_scenario_count": sum(1 for item in scenario_entries if item.get("status") == STATUS_DONE),
        "partial_scenario_count": sum(1 for item in scenario_entries if item.get("status") != STATUS_DONE),
        "scenarios": scenario_entries,
        "missing_required_items": missing_required,
        "scenario_blockers": scenario_blockers,
        "copied_files": copied_files,
        "claim_boundary": (
            "Phase1ReviewPack packages exact accepted comparison bundles for external audit. "
            "It is not Apollo natural-driving success evidence."
        ),
    }
    manifest_path = output / "phase1_review_pack_manifest.json"
    summary_path = output / "phase1_review_pack_summary.md"
    summary_path.write_text(_markdown(manifest), encoding="utf-8")
    copied_files.append(_file_record(summary_path, output, "phase1_review_pack_summary"))
    manifest["copied_files"] = copied_files
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def write_phase1_review_archive(pack_dir: str | Path, archive_path: str | Path | None = None) -> dict[str, str]:
    root = Path(pack_dir).expanduser()
    archive = Path(archive_path).expanduser() if archive_path else root.with_suffix(".tar.gz")
    archive.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(root, arcname=root.name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    sha_path = archive.with_suffix(archive.suffix + ".sha256")
    sha_path.write_text(f"{digest}  {archive.name}\n", encoding="utf-8")
    return {"archive": str(archive), "sha256": str(sha_path), "sha256_digest": digest}


def _copy_required(
    src: Path,
    dest: Path,
    role: str,
    copied_files: list[dict[str, Any]],
    missing_required: list[dict[str, Any]],
) -> None:
    if not src.exists() or not src.is_file():
        missing_required.append({"role": role, "source": str(src), "destination": str(dest)})
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)
    copied_files.append(_file_record(dest, dest.parents[1], role))


def _copy_tree(src_root: Path, dest_root: Path, copied_files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    if not src_root.exists() or not src_root.is_dir():
        return copied
    for src in sorted(path for path in src_root.rglob("*") if path.is_file()):
        rel = src.relative_to(src_root)
        dest = dest_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
        record = _file_record(dest, dest_root.parent.parent, "accepted_bundle_file")
        copied_files.append(record)
        copied.append(record)
    return copied


def _file_record(path: Path, pack_root: Path, role: str) -> dict[str, Any]:
    data = path.read_bytes()
    try:
        rel = path.relative_to(pack_root)
    except ValueError:
        rel = path
    return {
        "role": role,
        "path": str(rel),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
    }


def _bundle_self_contained(report: Mapping[str, Any]) -> bool:
    gates = report.get("gates") if isinstance(report.get("gates"), Mapping) else {}
    if gates.get("bundle_self_contained") is True:
        return True
    materialization = (
        report.get("bundle_materialization")
        if isinstance(report.get("bundle_materialization"), Mapping)
        else {}
    )
    return materialization.get("self_contained") is True


def _resolve_path(raw: str, repo_root: Path) -> Path:
    path = Path(raw).expanduser()
    return path if path.is_absolute() else repo_root / path


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _safe_name(value: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)
    return clean[:140] or "scenario"


def _markdown(manifest: Mapping[str, Any]) -> str:
    lines = [
        "# Phase 1 Review Pack",
        "",
        f"Status: `{manifest.get('status')}`",
        f"Scenario count: `{manifest.get('scenario_count')}`",
        f"Done scenario count: `{manifest.get('done_scenario_count')}`",
        f"Partial scenario count: `{manifest.get('partial_scenario_count')}`",
        "",
        "## Scenarios",
        "",
        "| Scenario | Status | Bundle | Apollo Run | PlanningControl Run | Blockers |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for scenario in manifest.get("scenarios") or []:
        if not isinstance(scenario, Mapping):
            continue
        blockers = ", ".join(str(item) for item in scenario.get("blocking_reasons") or []) or "-"
        lines.append(
            "| {scenario} | {status} | {bundle} | {apollo} | {planning} | {blockers} |".format(
                scenario=scenario.get("scenario"),
                status=scenario.get("status"),
                bundle=scenario.get("pack_relative_path") or "-",
                apollo=scenario.get("apollo_run_id") or "-",
                planning=scenario.get("planning_control_run_id") or "-",
                blockers=blockers,
            )
        )
    lines.extend(["", str(manifest.get("claim_boundary") or "")])
    return "\n".join(lines) + "\n"
