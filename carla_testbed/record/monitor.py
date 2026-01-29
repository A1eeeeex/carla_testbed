from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, Any, Optional, List


class SignalMonitor:
    """
    Lightweight monitor for sensor/control signal presence during a run.

    - `record_sensor(slot, timestamp)` to note a sensor sample arrival.
    - `record_control(topic, timestamp)` to note a control command arrival.
    - `summary()` returns last-seen timestamps and gaps.
    - `persist(path)` writes a small JSON report (not auto-called).
    """

    def __init__(self, snapshot_interval: int = 20):
        self.sensors_last: Dict[str, float] = {}
        self.controls_last: Dict[str, float] = {}
        self.start_time = time.time()
        self.snapshot_interval = max(1, snapshot_interval)
        self.snapshots: List[Dict[str, Any]] = []

    def record_sensor(self, slot: str, timestamp: Optional[float] = None):
        self.sensors_last[slot] = timestamp if timestamp is not None else time.time()

    def record_control(self, topic: str, timestamp: Optional[float] = None, ctrl: Optional[Dict[str, Any]] = None):
        ts = timestamp if timestamp is not None else time.time()
        self.controls_last[topic] = ts
        if ctrl is not None:
            self.controls_last[f"{topic}::last_cmd"] = ctrl

    def maybe_snapshot(self, tick: int, timestamp: float, ctrl: Optional[Dict[str, Any]] = None):
        if tick % self.snapshot_interval == 0:
            self.snapshots.append(
                {
                    "tick": tick,
                    "t": timestamp,
                    "controls": ctrl or {},
                    "sensors_seen": list(self.sensors_last.keys()),
                }
            )

    def summary(self) -> Dict[str, Any]:
        now = time.time()
        def _gap(last):
            return None if last is None else now - last
        return {
            "since_start_s": now - self.start_time,
            "sensors": {k: {"last_ts": v, "age_s": _gap(v)} for k, v in self.sensors_last.items()},
            "controls": {k: {"last_ts": v, "age_s": _gap(v)} for k, v in self.controls_last.items()},
            "snapshots": self.snapshots,
        }

    def persist(self, path: Path):
        import json
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.summary(), indent=2))
