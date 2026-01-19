from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List


def scan_frames(sensors_dir: Path) -> List[Dict]:
    frames = []
    if not sensors_dir.exists():
        return frames
    for sid_dir in sensors_dir.iterdir():
        if not sid_dir.is_dir():
            continue
        sensor_id = sid_dir.name
        for f in sorted(sid_dir.glob("*")):
            if not f.is_file():
                continue
            stem = f.stem
            if not stem.isdigit():
                continue
            frames.append({"frame_id": int(stem), "sensor_id": sensor_id, "path": str(f)})
    frames.sort(key=lambda x: x["frame_id"])
    return frames


def write_index(run_dir: Path) -> Path:
    frames_dir = run_dir / "sensors"
    idx_path = run_dir / "frames.jsonl"
    entries = scan_frames(frames_dir)
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    with idx_path.open("w") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")
    return idx_path
