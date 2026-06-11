from __future__ import annotations

from dataclasses import dataclass
import time
from pathlib import Path
from typing import Any, Optional


@dataclass
class CarlaClientManager:
    host: str = "localhost"
    port: int = 2000
    timeout: float = 30.0
    root: Optional[Path] = None

    def create_client(self) -> Any:
        try:
            import carla  # type: ignore
        except Exception as exc:
            raise RuntimeError("CARLA Python package is required to create a CARLA client") from exc

        last_err: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                client = carla.Client(self.host, self.port)
                client.set_timeout(self.timeout)
                # quick ping
                client.get_server_version()
                if attempt > 1:
                    print(f"[carla] connected on attempt {attempt}")
                return client
            except Exception as exc:
                last_err = exc
                print(f"[carla][WARN] connect attempt {attempt} failed: {exc}")
                time.sleep(2.0)
        raise RuntimeError(f"Failed to connect to CARLA at {self.host}:{self.port} after 3 attempts") from last_err
