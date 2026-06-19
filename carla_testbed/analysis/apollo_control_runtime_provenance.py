from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

SCHEMA_VERSION = "apollo_control_runtime_provenance.v1"
SET_SCHEMA_VERSION = "apollo_control_runtime_overlay_set_provenance.v1"
DEFAULT_RECORD_NAME = "lib_control_cmd_proto_mcc_bin.so"

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def analyze_apollo_control_runtime_provenance(
    *,
    source_path: str | Path | None = None,
    target_path: str | Path | None = None,
    overlay_manifest: str | Path | None = None,
    record_name: str = DEFAULT_RECORD_NAME,
    container: str | None = None,
    source_run: str | None = None,
    baseline_run: str | None = None,
    online_diff_report: str | None = None,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Compare source and target runtime files without mutating Apollo state."""

    record = _resolve_record(
        overlay_manifest=Path(overlay_manifest).expanduser() if overlay_manifest else None,
        record_name=record_name,
    )
    source = str(source_path or record.get("source") or "")
    target = str(target_path or record.get("target") or "")
    missing_fields: list[str] = []
    if not source:
        missing_fields.append("source_path")
    if not target:
        missing_fields.append("target_path")

    if container:
        source_probe = _probe_docker_file(container, source, runner=runner) if source else _missing_probe(source)
        target_probe = _probe_docker_file(container, target, runner=runner) if target else _missing_probe(target)
    else:
        source_probe = _probe_local_file(Path(source).expanduser()) if source else _missing_probe(source)
        target_probe = _probe_local_file(Path(target).expanduser()) if target else _missing_probe(target)

    same_content = _same_hash(source_probe, target_probe)
    finding = _finding(source_probe, target_probe, same_content)
    warnings = ["diagnostic_only_not_capability_evidence"]
    if container:
        warnings.append("docker_probe_read_only")
    if missing_fields:
        warnings.append("runtime_file_path_missing")

    return {
        "schema_version": SCHEMA_VERSION,
        "created_at_epoch": time.time(),
        "container": container,
        "record_name": record_name,
        "source_run": source_run,
        "baseline_run": baseline_run,
        "online_diff_report": online_diff_report,
        "overlay_manifest": str(overlay_manifest) if overlay_manifest else None,
        "overlay_record": record,
        "source_probe": source_probe,
        "target_probe_current_after_restore": target_probe,
        "source_target_same_content_current_after_restore": same_content,
        "source_target_differences": _source_target_differences(source_probe, target_probe),
        "missing_fields": missing_fields,
        "warnings": warnings,
        "diagnostic_interpretation": {
            "status": "diagnostic_only"
            if not missing_fields and source_probe.get("exists") and target_probe.get("exists")
            else "insufficient_data",
            "finding": finding,
            "claim_boundary": (
                "Read-only runtime provenance only. This does not prove driving success "
                "and does not make the overlay default."
            ),
            "next_action": (
                "If the source differs from current target, inspect protobuf/control "
                "command ABI compatibility and test only with explicit diagnostic overlay gates."
            ),
        },
    }


def write_apollo_control_runtime_provenance(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    report_path = out / "control_cmd_proto_provenance.json"
    summary_path = out / "control_cmd_proto_provenance.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_apollo_control_runtime_provenance_summary(report), encoding="utf-8")
    return {"report": str(report_path), "summary": str(summary_path)}


def analyze_apollo_control_runtime_overlay_set_provenance(
    *,
    overlay_manifest: str | Path,
    container: str | None = None,
    source_run: str | None = None,
    baseline_run: str | None = None,
    online_diff_report: str | None = None,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    """Probe every overlay record and compare source files with current targets.

    This is intentionally read-only. It explains the runtime overlay condition
    used by online diagnostics; it does not mutate Apollo and does not make the
    overlay claim-grade or default.
    """

    manifest_path = Path(overlay_manifest).expanduser()
    records = _resolve_records(manifest_path)
    missing_fields: list[str] = []
    if not records:
        missing_fields.append("overlay_manifest.records")

    record_reports: list[dict[str, Any]] = []
    for record in records:
        source = str(record.get("source") or "")
        target = str(record.get("target") or "")
        record_missing: list[str] = []
        if not source:
            record_missing.append("source")
        if not target:
            record_missing.append("target")
        if container:
            source_probe = _probe_docker_file(container, source, runner=runner) if source else _missing_probe(source)
            target_probe = _probe_docker_file(container, target, runner=runner) if target else _missing_probe(target)
        else:
            source_probe = _probe_local_file(Path(source).expanduser()) if source else _missing_probe(source)
            target_probe = _probe_local_file(Path(target).expanduser()) if target else _missing_probe(target)
        same_content = _same_hash(source_probe, target_probe)
        diffs = _source_target_differences(source_probe, target_probe)
        record_reports.append(
            {
                "name": record.get("name"),
                "source": source,
                "target": target,
                "source_probe": source_probe,
                "target_probe_current_after_restore": target_probe,
                "source_target_same_content_current_after_restore": same_content,
                "source_target_differences": diffs,
                "finding": _finding(source_probe, target_probe, same_content),
                "missing_fields": record_missing,
            }
        )

    summary = _set_provenance_summary(record_reports)
    warnings = ["diagnostic_only_not_capability_evidence"]
    if container:
        warnings.append("docker_probe_read_only")
    if missing_fields:
        warnings.append("overlay_manifest_records_missing")

    return {
        "schema_version": SET_SCHEMA_VERSION,
        "created_at_epoch": time.time(),
        "container": container,
        "source_run": source_run,
        "baseline_run": baseline_run,
        "online_diff_report": online_diff_report,
        "overlay_manifest": str(overlay_manifest),
        "record_count": len(records),
        "records": record_reports,
        "summary": summary,
        "missing_fields": missing_fields,
        "warnings": warnings,
        "diagnostic_interpretation": {
            "status": "diagnostic_only"
            if records and summary["missing_pair_count"] == 0
            else "insufficient_data",
            "finding": _set_finding(summary, missing_fields),
            "claim_boundary": (
                "Read-only overlay-set provenance only. This can support a diagnostic "
                "hypothesis about Apollo control runtime compatibility, but it is not "
                "Apollo driving evidence and does not make the overlay default."
            ),
            "next_action": _set_next_action(summary),
        },
    }


def write_apollo_control_runtime_overlay_set_provenance(
    report: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, str]:
    out = Path(out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    report_path = out / "apollo_control_runtime_overlay_set_provenance_report.json"
    summary_path = out / "apollo_control_runtime_overlay_set_provenance_summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_apollo_control_runtime_overlay_set_provenance_summary(report), encoding="utf-8")
    return {"report": str(report_path), "summary": str(summary_path)}


def render_apollo_control_runtime_provenance_summary(report: Mapping[str, Any]) -> str:
    source = report.get("source_probe") if isinstance(report.get("source_probe"), Mapping) else {}
    target = (
        report.get("target_probe_current_after_restore")
        if isinstance(report.get("target_probe_current_after_restore"), Mapping)
        else {}
    )
    interpretation = (
        report.get("diagnostic_interpretation")
        if isinstance(report.get("diagnostic_interpretation"), Mapping)
        else {}
    )
    source_stat = source.get("stat_fields") if isinstance(source.get("stat_fields"), Mapping) else {}
    target_stat = target.get("stat_fields") if isinstance(target.get("stat_fields"), Mapping) else {}
    lines = [
        "# Control Cmd Proto Runtime Provenance",
        "",
        f"- container: `{report.get('container')}`",
        f"- record_name: `{report.get('record_name')}`",
        f"- overlay source: `{source.get('path')}`",
        f"- current target after restore: `{target.get('path')}`",
        f"- source exists: `{source.get('exists')}`",
        f"- target exists: `{target.get('exists')}`",
        f"- source sha256: `{source.get('sha256')}`",
        f"- target sha256: `{target.get('sha256')}`",
        f"- source size bytes: `{source_stat.get('size_bytes')}`",
        f"- target size bytes: `{target_stat.get('size_bytes')}`",
        f"- source elf path style: `{source.get('elf_path_style')}`",
        f"- target elf path style: `{target.get('elf_path_style')}`",
        f"- source_target_same_content_current_after_restore: `{report.get('source_target_same_content_current_after_restore')}`",
        f"- interpretation: `{interpretation.get('finding')}`",
        "",
        "## Boundary",
        "",
        str(interpretation.get("claim_boundary") or ""),
    ]
    diffs = report.get("source_target_differences")
    if isinstance(diffs, list) and diffs:
        lines.extend(["", "## Differences", ""])
        lines.extend(f"- `{item}`" for item in diffs)
    warnings = report.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in warnings)
    return "\n".join(lines) + "\n"


def render_apollo_control_runtime_overlay_set_provenance_summary(report: Mapping[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
    interpretation = (
        report.get("diagnostic_interpretation")
        if isinstance(report.get("diagnostic_interpretation"), Mapping)
        else {}
    )
    lines = [
        "# Apollo Control Runtime Overlay Set Provenance",
        "",
        f"- container: `{report.get('container')}`",
        f"- overlay_manifest: `{report.get('overlay_manifest')}`",
        f"- record_count: `{report.get('record_count')}`",
        f"- existing_pair_count: `{summary.get('existing_pair_count')}`",
        f"- missing_pair_count: `{summary.get('missing_pair_count')}`",
        f"- same_content_count: `{summary.get('same_content_count')}`",
        f"- different_content_count: `{summary.get('different_content_count')}`",
        f"- hash_unknown_count: `{summary.get('hash_unknown_count')}`",
        f"- records_with_elf_path_style_diff: `{summary.get('records_with_elf_path_style_diff')}`",
        f"- records_with_needed_libraries_diff: `{summary.get('records_with_needed_libraries_diff')}`",
        f"- interpretation: `{interpretation.get('finding')}`",
        "",
        "## Boundary",
        "",
        str(interpretation.get("claim_boundary") or ""),
    ]
    records = report.get("records")
    if isinstance(records, list) and records:
        lines.extend(["", "## Records", ""])
        for item in records:
            if not isinstance(item, Mapping):
                continue
            diffs = item.get("source_target_differences")
            diff_text = ", ".join(str(v) for v in diffs) if isinstance(diffs, list) else ""
            lines.append(
                f"- `{item.get('name')}`: finding=`{item.get('finding')}`, "
                f"same_content=`{item.get('source_target_same_content_current_after_restore')}`, "
                f"diffs=`{diff_text}`"
            )
    warnings = report.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in warnings)
    next_action = interpretation.get("next_action")
    if next_action:
        lines.extend(["", "## Next Action", "", str(next_action)])
    return "\n".join(lines) + "\n"


def _resolve_record(*, overlay_manifest: Path | None, record_name: str) -> dict[str, Any]:
    if not overlay_manifest:
        return {}
    try:
        payload = json.loads(overlay_manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    records = payload.get("records")
    if not isinstance(records, list):
        return {}
    for item in records:
        if isinstance(item, Mapping) and item.get("name") == record_name:
            return {str(k): v for k, v in item.items()}
    return {}


def _resolve_records(overlay_manifest: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(overlay_manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    records = payload.get("records")
    if not isinstance(records, list):
        return []
    return [{str(k): v for k, v in item.items()} for item in records if isinstance(item, Mapping)]


def _probe_local_file(path: Path) -> dict[str, Any]:
    payload = _base_probe(str(path), exists=path.exists())
    if not path.exists():
        return payload
    try:
        stat = path.stat()
        data = path.read_bytes()
    except OSError as exc:
        payload["warnings"].append(f"local_probe_failed:{exc.__class__.__name__}")
        return payload
    payload.update(
        {
            "sha256": hashlib.sha256(data).hexdigest(),
            "stat_fields": {
                "path": str(path),
                "size_bytes": stat.st_size,
                "mode": oct(stat.st_mode & 0o777),
                "mtime_epoch": int(stat.st_mtime),
                "file_type": "regular file" if path.is_file() else "other",
            },
            "file_summary": _local_file_summary(data),
            "elf_path_style": _elf_path_style_from_text(data.decode("latin1", errors="ignore")),
        }
    )
    return payload


def _probe_docker_file(
    container: str,
    path: str,
    *,
    runner: CommandRunner | None = None,
) -> dict[str, Any]:
    payload = _base_probe(path, exists=False)
    run = runner or _run_subprocess
    commands = {
        "exists": f"test -e {json.dumps(path)}",
        "stat": f"stat -Lc \"%n|%s|%F|%a|%U|%G|%Y\" {json.dumps(path)} 2>&1",
        "sha256sum": f"sha256sum {json.dumps(path)} 2>&1",
        "file": f"file -L {json.dumps(path)} 2>&1",
        "readelf_dynamic": f"readelf -d {json.dumps(path)} 2>&1 | grep -E \"NEEDED|RPATH|RUNPATH|SONAME\" || true",
        "symbols_control_cmd": (
            f"(nm -D {json.dumps(path)} 2>/dev/null || true; "
            f"readelf -Ws {json.dumps(path)} 2>/dev/null || true) "
            "| grep -Ei \"control.?cmd|ControlCommand|apollo.*control|common_msgs\" | head -n 80 || true"
        ),
        "strings_control_cmd": (
            f"strings {json.dumps(path)} 2>/dev/null "
            "| grep -Ei \"control.?cmd|ControlCommand|apollo.*control|common_msgs|protobuf|proto\" "
            "| head -n 120 || true"
        ),
    }
    results = {name: _run_docker(container, cmd, run) for name, cmd in commands.items()}
    payload["commands"] = results
    payload["exists"] = results["exists"]["returncode"] == 0
    payload["sha256"] = _parse_sha256(results["sha256sum"].get("stdout", ""))
    payload["stat_fields"] = _parse_stat(results["stat"].get("stdout", ""))
    payload["file_summary"] = results["file"].get("stdout", "").strip()
    dynamic_text = results["readelf_dynamic"].get("stdout", "")
    payload["elf_path_style"] = _elf_path_style_from_text(dynamic_text)
    payload["needed_libraries"] = _parse_needed_libraries(dynamic_text)
    return payload


def _base_probe(path: str, *, exists: bool) -> dict[str, Any]:
    return {
        "path": path,
        "exists": exists,
        "sha256": None,
        "stat_fields": {},
        "file_summary": "",
        "elf_path_style": "unknown",
        "needed_libraries": [],
        "warnings": [],
    }


def _run_docker(container: str, command: str, runner: CommandRunner) -> dict[str, Any]:
    result = runner(["docker", "exec", container, "bash", "-lc", command])
    return {
        "cmd": command,
        "returncode": int(result.returncode),
        "stdout": result.stdout or "",
        "stderr": result.stderr or "",
    }


def _run_subprocess(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
        check=False,
    )


def _local_file_summary(data: bytes) -> str:
    if data.startswith(b"\x7fELF"):
        return "ELF binary"
    if data.startswith(b"#!"):
        return "script"
    return "binary" if b"\x00" in data[:1024] else "text"


def _elf_path_style_from_text(text: str) -> str:
    has_runpath = "RUNPATH" in text
    has_rpath = "RPATH" in text
    if has_runpath and has_rpath:
        return "rpath_and_runpath"
    if has_runpath:
        return "runpath"
    if has_rpath:
        return "rpath"
    return "unknown"


def _parse_needed_libraries(text: str) -> list[str]:
    libs: list[str] = []
    for line in text.splitlines():
        if "Shared library:" not in line:
            continue
        _, _, tail = line.partition("[")
        lib, _, _ = tail.partition("]")
        if lib:
            libs.append(lib)
    return libs


def _parse_sha256(text: str) -> str | None:
    parts = text.strip().split()
    if not parts:
        return None
    value = parts[0]
    if len(value) == 64 and all(ch in "0123456789abcdefABCDEF" for ch in value):
        return value.lower()
    return None


def _parse_stat(text: str) -> dict[str, Any]:
    line = text.strip().splitlines()[0] if text.strip() else ""
    parts = line.split("|")
    if len(parts) < 7:
        return {}
    try:
        size = int(parts[1])
        mtime = int(parts[6])
    except ValueError:
        return {}
    return {
        "path": parts[0],
        "size_bytes": size,
        "file_type": parts[2],
        "mode": parts[3],
        "owner": parts[4],
        "group": parts[5],
        "mtime_epoch": mtime,
    }


def _same_hash(source_probe: Mapping[str, Any], target_probe: Mapping[str, Any]) -> bool | None:
    source_hash = source_probe.get("sha256")
    target_hash = target_probe.get("sha256")
    if not source_hash or not target_hash:
        return None
    return str(source_hash) == str(target_hash)


def _source_target_differences(source_probe: Mapping[str, Any], target_probe: Mapping[str, Any]) -> list[str]:
    diffs: list[str] = []
    if _same_hash(source_probe, target_probe) is False:
        diffs.append("sha256_differs")
    source_stat = source_probe.get("stat_fields") if isinstance(source_probe.get("stat_fields"), Mapping) else {}
    target_stat = target_probe.get("stat_fields") if isinstance(target_probe.get("stat_fields"), Mapping) else {}
    if source_stat.get("size_bytes") != target_stat.get("size_bytes"):
        diffs.append("size_differs")
    if source_probe.get("elf_path_style") != target_probe.get("elf_path_style"):
        diffs.append("elf_path_style_differs")
    source_needed = set(source_probe.get("needed_libraries") or [])
    target_needed = set(target_probe.get("needed_libraries") or [])
    if source_needed and target_needed and source_needed != target_needed:
        diffs.append("needed_libraries_differ")
    return diffs


def _finding(source_probe: Mapping[str, Any], target_probe: Mapping[str, Any], same_content: bool | None) -> str:
    if not source_probe.get("exists") or not target_probe.get("exists"):
        return "source_or_target_missing"
    if same_content is True:
        return "overlay_source_matches_current_target"
    if same_content is False:
        return "overlay_source_differs_from_current_target"
    return "insufficient_hash_data"


def _set_provenance_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    existing_pair_count = 0
    missing_pair_count = 0
    same_content_count = 0
    different_content_count = 0
    hash_unknown_count = 0
    records_with_elf_path_style_diff = 0
    records_with_needed_libraries_diff = 0
    for record in records:
        source_probe = record.get("source_probe") if isinstance(record.get("source_probe"), Mapping) else {}
        target_probe = (
            record.get("target_probe_current_after_restore")
            if isinstance(record.get("target_probe_current_after_restore"), Mapping)
            else {}
        )
        if source_probe.get("exists") and target_probe.get("exists"):
            existing_pair_count += 1
        else:
            missing_pair_count += 1
        same = record.get("source_target_same_content_current_after_restore")
        if same is True:
            same_content_count += 1
        elif same is False:
            different_content_count += 1
        else:
            hash_unknown_count += 1
        diffs = record.get("source_target_differences")
        if isinstance(diffs, list):
            if "elf_path_style_differs" in diffs:
                records_with_elf_path_style_diff += 1
            if "needed_libraries_differ" in diffs:
                records_with_needed_libraries_diff += 1
    return {
        "existing_pair_count": existing_pair_count,
        "missing_pair_count": missing_pair_count,
        "same_content_count": same_content_count,
        "different_content_count": different_content_count,
        "hash_unknown_count": hash_unknown_count,
        "records_with_elf_path_style_diff": records_with_elf_path_style_diff,
        "records_with_needed_libraries_diff": records_with_needed_libraries_diff,
    }


def _set_finding(summary: Mapping[str, int], missing_fields: Sequence[str]) -> str:
    if missing_fields:
        return "overlay_manifest_records_missing"
    if summary.get("missing_pair_count", 0) > 0:
        return "overlay_set_partially_missing"
    if summary.get("different_content_count", 0) > 0:
        return "overlay_set_differs_from_current_targets"
    if summary.get("same_content_count", 0) > 0 and summary.get("hash_unknown_count", 0) == 0:
        return "overlay_set_matches_current_targets"
    return "insufficient_hash_data"


def _set_next_action(summary: Mapping[str, int]) -> str:
    if summary.get("missing_pair_count", 0) > 0:
        return "Resolve missing source/target runtime files before attributing online control survival."
    if summary.get("different_content_count", 0) > 0:
        return (
            "Inspect the differing overlay files as a runtime ABI/configuration set; "
            "do not promote the overlay until its provenance and necessity are explained."
        )
    return "If online behavior still differs, inspect process logs and dynamic loader state next."
