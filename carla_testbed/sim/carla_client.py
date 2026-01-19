from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import carla


def _ensure_pythonapi_on_path(root: Path):
    paths = [
        root / "PythonAPI",
        root / "PythonAPI" / "carla",
    ]
    for p in paths:
        if p.exists() and str(p) not in sys.path:
            sys.path.insert(0, str(p))


@dataclass
class CarlaClientManager:
    host: str = "localhost"
    port: int = 2000
    timeout: float = 30.0
    root: Optional[Path] = None

    def create_client(self) -> carla.Client:
        if self.root is not None:
            _ensure_pythonapi_on_path(self.root)
        client = carla.Client(self.host, self.port)
        client.set_timeout(self.timeout)
        return client
