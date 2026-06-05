from __future__ import annotations

import tarfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PackageResult:
    output_path: Path
    included_files: list[str]
    skipped_files: list[str]

    def to_dict(self) -> dict:
        return {
            "output_path": str(self.output_path),
            "included_files": list(self.included_files),
            "skipped_files": list(self.skipped_files),
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
    candidates = _candidate_files(root)
    with tarfile.open(output, "w:gz") as archive:
        for path in candidates:
            rel = path.relative_to(root)
            rel_text = str(rel)
            if not include_large_artifacts and any(part in LARGE_DIR_PARTS for part in rel.parts):
                skipped.append(rel_text)
                continue
            archive.add(path, arcname=str(Path(root.name) / rel))
            included.append(rel_text)
    return PackageResult(output_path=output, included_files=included, skipped_files=skipped)


def _candidate_files(root: Path) -> list[Path]:
    files: dict[Path, None] = {}
    for pattern in DEFAULT_INCLUDE_PATTERNS:
        for path in root.glob(pattern):
            if path.is_file():
                files[path] = None
    return sorted(files)
