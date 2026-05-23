from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_ROOT = REPO_ROOT / "configs"
FORBIDDEN_PATH_RE = re.compile(r"(/home/|/Users/|C:\\Users|/mnt/data|/tmp/)")


def test_tracked_yaml_configs_do_not_embed_machine_paths() -> None:
    offenders = []
    for path in sorted(CONFIG_ROOT.rglob("*")):
        if path.suffix not in {".yaml", ".yml"}:
            continue
        text = path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if FORBIDDEN_PATH_RE.search(line):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{line_no}: {line.strip()}")

    assert not offenders, "hardcoded machine paths found:\n" + "\n".join(offenders)
