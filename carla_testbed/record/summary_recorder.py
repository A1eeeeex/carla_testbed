from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class SummaryRecorder:
    def __init__(self, path: Path):
        self.path = path

    def write(self, summary: Dict[str, Any]):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(summary, indent=2))
