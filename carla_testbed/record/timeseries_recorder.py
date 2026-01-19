from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Any


class TimeseriesRecorder:
    """Minimal CSV recorder mirroring legacy fields subset."""

    def __init__(self, path: Path):
        self.path = path
        self.f = path.open("w", newline="")
        self.writer = None

    def close(self):
        try:
            self.f.close()
        except Exception:
            pass

    def write_row(self, row: Dict[str, Any]):
        if self.writer is None:
            headers = list(row.keys())
            self.writer = csv.DictWriter(self.f, fieldnames=headers)
            self.writer.writeheader()
        self.writer.writerow(row)
        self.f.flush()
