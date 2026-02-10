from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable


def _slug(text: str) -> str:
    s = (text or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "na"


def build_run_name(timestamp: str, parts: Iterable[str]) -> str:
    fields = [timestamp] + [_slug(p) for p in parts if p]
    return "__".join(fields)


def next_available_run_dir(runs_root: Path, run_name: str) -> Path:
    candidate = runs_root / run_name
    if not candidate.exists():
        return candidate
    idx = 2
    while True:
        alt = runs_root / f"{run_name}__{idx:02d}"
        if not alt.exists():
            return alt
        idx += 1


def update_latest_pointer(run_dir: Path) -> None:
    runs_root = run_dir.parent
    latest_link = runs_root / "latest"
    if latest_link.exists() and latest_link.is_dir() and (not latest_link.is_symlink()):
        latest_link = runs_root / "latest_run"
    latest_txt = runs_root / "LATEST.txt"
    try:
        if latest_link.is_symlink() or latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(run_dir.name)
    except Exception:
        pass
    try:
        latest_txt.write_text(str(run_dir.resolve()) + "\n", encoding="utf-8")
    except Exception:
        pass
