from __future__ import annotations

import numbers
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
        self.controls_last: Dict[str, Any] = {}
        self.controls_cmd: Dict[str, Dict[str, Any]] = {}
        self.start_time = time.time()
        self.snapshot_interval = max(1, snapshot_interval)
        self.snapshots: List[Dict[str, Any]] = []

    def record_sensor(self, slot: str, timestamp: Optional[float] = None):
        self.sensors_last[slot] = timestamp if timestamp is not None else time.time()

    def record_control(self, topic: str, timestamp: Optional[float] = None, ctrl: Optional[Dict[str, Any]] = None):
        ts = timestamp if timestamp is not None else time.time()
        self.controls_last[topic] = ts
        if ctrl is not None:
            self.controls_cmd[topic] = ctrl

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
            return (now - float(last)) if isinstance(last, numbers.Real) else None

        controls = {}
        for k, v in self.controls_last.items():
            entry = {"last_ts": v, "age_s": _gap(v)}
            if k in self.controls_cmd:
                entry["last_cmd"] = self.controls_cmd[k]
            controls[k] = entry
        return {
            "since_start_s": now - self.start_time,
            "sensors": {k: {"last_ts": v, "age_s": _gap(v)} for k, v in self.sensors_last.items()},
            "controls": controls,
            "snapshots": self.snapshots,
        }

    def persist(self, path: Path):
        import json
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.summary(), indent=2))
