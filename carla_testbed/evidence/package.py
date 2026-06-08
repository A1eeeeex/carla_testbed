from __future__ import annotations

import tarfile
import json
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path


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
    "artifacts/topic_publish_stats.jsonl",
    "artifacts/publish_gap_trace.jsonl",
    "artifacts/control_apply_trace.jsonl",
    "artifacts/planning_topic_debug.jsonl",
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
    "artifacts/control_apply_trace.jsonl",
    "artifacts/planning_topic_debug.jsonl",
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
    return [pattern for pattern in CLAIM_REQUIRED_ROW_LEVEL if not (root / pattern).exists()]


def _claim_reproducibility_level(profile: str, missing_required: list[str]) -> str:
    if profile != "claim":
        return f"{profile}_evidence"
    if missing_required:
        return "summary_only_missing_row_level"
    return "row_level_evidence_present"


def _add_bytes(archive: tarfile.TarFile, arcname: Path, text: str) -> None:
    data = text.encode("utf-8")
    info = tarfile.TarInfo(str(arcname))
    info.size = len(data)
    archive.addfile(info, BytesIO(data))
