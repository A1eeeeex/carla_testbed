from __future__ import annotations

import hashlib
import tarfile
import json
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PackageResult:
    output_path: Path
    included_files: list[str]
    skipped_files: list[str]
    missing_required_files: list[str]
    omitted_large_artifacts: list[str]
    claim_reproducibility_level: str
    status: str

    def to_dict(self) -> dict:
        return {
            "output_path": str(self.output_path),
            "included_files": list(self.included_files),
            "skipped_files": list(self.skipped_files),
            "missing_required_files": list(self.missing_required_files),
            "omitted_large_artifacts": list(self.omitted_large_artifacts),
            "claim_reproducibility_level": self.claim_reproducibility_level,
            "status": self.status,
        }


DEFAULT_INCLUDE_PATTERNS = (
    "manifest.json",
    "summary.json",
    "config.resolved.yaml",
    "*.plan.resolved.yaml",
    "analysis/**/*.json",
    "analysis/**/*.md",
    "artifacts/*summary*.json",
    "artifacts/*stats*.json",
    "artifacts/*contract*.json",
    "artifacts/*inspection*.json",
    "artifacts/traffic_flow_manifest.json",
)

ROW_LEVEL_EVIDENCE_PATTERNS = (
    "artifacts/fixed_scene_resolved.json",
    "artifacts/fixed_scene_runtime_state.json",
    "artifacts/scenario_actor_trace.jsonl",
    "artifacts/scenario_phase_events.jsonl",
    "artifacts/ego_control_trace.jsonl",
    "artifacts/traffic_spawn_candidates.jsonl",
    "artifacts/walker_flow_trace.jsonl",
    "artifacts/topic_publish_stats.jsonl",
    "artifacts/publish_gap_trace.jsonl",
    "artifacts/control_apply_trace.jsonl",
    "artifacts/planning_topic_debug.jsonl",
    "artifacts/routing_event_debug.jsonl",
    "artifacts/planning_route_segment_debug.jsonl",
    "artifacts/control_decode_debug.jsonl",
    "artifacts/apollo_reference_line_contract.jsonl",
    "artifacts/apollo_hdmap_projection.jsonl",
    "artifacts/obstacle_gt_contract.jsonl",
    "artifacts/traffic_light_contract.jsonl",
    "artifacts/traffic_flow_events.jsonl",
    "artifacts/walker_spawn_candidates.jsonl",
    "analysis/**/apollo_reference_line_contract.jsonl",
    "analysis/**/apollo_hdmap_projection.jsonl",
    "analysis/**/obstacle_gt_contract.jsonl",
    "analysis/**/traffic_light_contract.jsonl",
)

CLAIM_REQUIRED_ROW_LEVEL = (
    "artifacts/topic_publish_stats.jsonl",
    "artifacts/publish_gap_trace.jsonl",
    "artifacts/routing_event_debug.jsonl",
    "artifacts/planning_route_segment_debug.jsonl",
    "artifacts/control_apply_trace.jsonl",
    "artifacts/control_decode_debug.jsonl",
    "artifacts/planning_topic_debug.jsonl",
)

FIXED_SCENE_CLAIM_REQUIRED_ROW_LEVEL = (
    "artifacts/fixed_scene_resolved.json",
    "artifacts/fixed_scene_runtime_state.json",
    "artifacts/scenario_actor_trace.jsonl",
    "artifacts/scenario_phase_events.jsonl",
)

LOG_INDEX_PATTERNS = (
    "logs/**/*.txt",
    "logs/**/*.log",
    "logs/**/*summary*",
    "artifacts/*log*index*.json",
)

LARGE_DIR_PARTS = {"video", "rosbag2", "cyber_record", "recordings"}


def package_run_evidence(
    run_dir: str | Path,
    *,
    out_path: str | Path,
    profile: str = "claim",
    include_large_artifacts: bool = False,
) -> PackageResult:
    root = Path(run_dir).expanduser()
    output = Path(out_path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    included: list[str] = []
    skipped: list[str] = []
    omitted_large: list[str] = []
    candidates = _candidate_files(root, profile=profile)
    missing_required = _missing_required(root, profile=profile)
    reproducibility = _claim_reproducibility_level(profile, missing_required)
    status = "pass" if not missing_required else "insufficient_data"
    with tarfile.open(output, "w:gz") as archive:
        for path in candidates:
            rel = path.relative_to(root)
            rel_text = str(rel)
            if not include_large_artifacts and any(part in LARGE_DIR_PARTS for part in rel.parts):
                skipped.append(rel_text)
                omitted_large.append(rel_text)
                continue
            archive.add(path, arcname=str(Path(root.name) / rel))
            included.append(rel_text)
        row_level_index, row_level_samples = _row_level_evidence_index(root)
        if row_level_index:
            _add_bytes(
                archive,
                Path(root.name) / "row_level_evidence_index.json",
                json.dumps(row_level_index, indent=2, sort_keys=True) + "\n",
            )
            included.append("row_level_evidence_index.json")
            for sample_rel, sample_text in sorted(row_level_samples.items()):
                _add_bytes(archive, Path(root.name) / sample_rel, sample_text)
                included.append(sample_rel)
        manifest = {
            "schema_version": "evidence_package_manifest.v1",
            "profile": profile,
            "run_dir": str(root),
            "included_files": sorted(included),
            "skipped_files": sorted(skipped),
            "missing_required_files": sorted(missing_required),
            "omitted_large_artifacts": sorted(omitted_large),
            "claim_reproducibility_level": reproducibility,
            "status": status,
            "row_level_evidence_index": "row_level_evidence_index.json" if row_level_index else None,
            "source_context_requirements": _source_context_requirements(root),
            "claim_boundary": (
                "Package completeness is review evidence only. It cannot turn a run into "
                "natural-driving pass without gate_report.json and natural_driving_report.json."
            ),
        }
        _add_bytes(archive, Path(root.name) / "package_manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        included.append("package_manifest.json")
    return PackageResult(
        output_path=output,
        included_files=sorted(included),
        skipped_files=sorted(skipped),
        missing_required_files=sorted(missing_required),
        omitted_large_artifacts=sorted(omitted_large),
        claim_reproducibility_level=reproducibility,
        status=status,
    )


def _source_context_requirements(root: Path) -> list[str]:
    manifest = _read_json(root / "manifest.json")
    summary = _read_json(root / "summary.json")
    runtime_dispatch = str(
        manifest.get("runtime_dispatch_kind")
        or summary.get("runtime_dispatch_kind")
        or ""
    )
    transport = str(manifest.get("transport_mode") or summary.get("transport_mode") or "")
    compat_layers = [str(item) for item in (manifest.get("compat_layers") or [])]
    transition = (
        runtime_dispatch == "typed_apollo_claim_runtime"
        or "ros2_gt_transition" in compat_layers
        or "legacy_route_health_transition" in compat_layers
        or transport == "apollo_cyberrt_gt_over_ros2_transition"
    )
    if not transition:
        return []
    return [
        "examples/",
        "configs/io/",
        "carla_testbed/runtime/",
        "tbio/",
        "tools/apollo10_cyber_bridge/",
    ]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _candidate_files(root: Path, *, profile: str) -> list[Path]:
    files: dict[Path, None] = {}
    patterns = list(DEFAULT_INCLUDE_PATTERNS)
    if profile in {"claim", "debug"}:
        patterns.extend(ROW_LEVEL_EVIDENCE_PATTERNS)
        patterns.extend(LOG_INDEX_PATTERNS)
    if profile == "demo":
        patterns.extend(["video/**/*.mp4", "video/**/*.json", "video/**/*.md"])
    for pattern in patterns:
        for path in root.glob(pattern):
            if path.is_file():
                files[path] = None
    return sorted(files)


def _missing_required(root: Path, *, profile: str) -> list[str]:
    if profile != "claim":
        return []
    required = list(CLAIM_REQUIRED_ROW_LEVEL)
    if _fixed_scene_enabled(root):
        required.extend(FIXED_SCENE_CLAIM_REQUIRED_ROW_LEVEL)
    return [pattern for pattern in required if not (root / pattern).exists()]


def _claim_reproducibility_level(profile: str, missing_required: list[str]) -> str:
    if profile != "claim":
        return f"{profile}_evidence"
    if any(pattern in FIXED_SCENE_CLAIM_REQUIRED_ROW_LEVEL for pattern in missing_required):
        return "summary_only_missing_fixed_scene_row_level"
    if missing_required:
        return "summary_only_missing_row_level"
    return "row_level_evidence_present"


def _row_level_evidence_index(
    root: Path,
    *,
    sample_lines: int = 5,
) -> tuple[dict[str, Any], dict[str, str]]:
    rows: list[dict[str, Any]] = []
    samples: dict[str, str] = {}
    seen: set[Path] = set()
    for pattern in ROW_LEVEL_EVIDENCE_PATTERNS:
        for path in sorted(root.glob(pattern)):
            if not path.is_file() or path.suffix != ".jsonl" or path in seen:
                continue
            seen.add(path)
            rel = path.relative_to(root)
            entry, head, tail = _jsonl_evidence_entry(path, rel, sample_lines=sample_lines)
            rows.append(entry)
            sample_base = Path("row_level_samples") / rel
            if head:
                head_rel = str(sample_base.with_suffix(sample_base.suffix + ".head.jsonl"))
                samples[head_rel] = "".join(f"{line}\n" for line in head)
                entry["head_sample"] = head_rel
            if tail:
                tail_rel = str(sample_base.with_suffix(sample_base.suffix + ".tail.jsonl"))
                samples[tail_rel] = "".join(f"{line}\n" for line in tail)
                entry["tail_sample"] = tail_rel
    if not rows:
        return {}, {}
    return {
        "schema_version": "row_level_evidence_index.v1",
        "sample_line_count": sample_lines,
        "files": rows,
        "claim_boundary": (
            "Head/tail samples and hashes support external review. They summarize row-level evidence "
            "but do not replace analyzer reports or natural-driving gates."
        ),
    }, samples


def _jsonl_evidence_entry(
    path: Path,
    rel: Path,
    *,
    sample_lines: int,
) -> tuple[dict[str, Any], list[str], list[str]]:
    digest = hashlib.sha256()
    head: list[str] = []
    tail: list[str] = []
    row_count = 0
    first_time = None
    last_time = None
    fields: set[str] = set()
    with path.open("rb") as handle:
        for raw_line in handle:
            digest.update(raw_line)
            line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
            if not line.strip():
                continue
            row_count += 1
            if len(head) < sample_lines:
                head.append(line)
            tail.append(line)
            if len(tail) > sample_lines:
                tail.pop(0)
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                fields.update(str(key) for key in payload.keys())
                row_time = _row_time(payload)
                if row_time is not None:
                    if first_time is None:
                        first_time = row_time
                    last_time = row_time
    return (
        {
            "path": str(rel),
            "sha256": digest.hexdigest(),
            "size_bytes": path.stat().st_size,
            "row_count": row_count,
            "time_start_sec": first_time,
            "time_end_sec": last_time,
            "top_level_fields": sorted(fields)[:80],
        },
        head,
        tail,
    )


def _row_time(payload: dict[str, Any]) -> float | None:
    for key in (
        "sim_time_sec",
        "sim_time",
        "timestamp",
        "timestamp_sec",
        "wall_time_sec",
        "time_sec",
    ):
        value = payload.get(key)
        if value in {None, ""}:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _fixed_scene_enabled(root: Path) -> bool:
    if (root / "artifacts" / "fixed_scene_resolved.json").exists() or (root / "fixed_scene_resolved.json").exists():
        return True
    for rel in ("manifest.json", "summary.json"):
        path = root / rel
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        fixed_scene = payload.get("fixed_scene")
        if isinstance(fixed_scene, dict) and fixed_scene.get("enabled", True):
            return True
        if payload.get("fixed_scene_enabled") is True:
            return True
    return False


def _add_bytes(archive: tarfile.TarFile, arcname: Path, text: str) -> None:
    data = text.encode("utf-8")
    info = tarfile.TarInfo(str(arcname))
    info.size = len(data)
    archive.addfile(info, BytesIO(data))
