#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = REPO_ROOT / "artifacts" / "dreamview_capture_region_cache.json"


def _load_yaml(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text()) or {}
    return data if isinstance(data, dict) else {}


def _record_cfg_from_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    return (((profile.get("algo") or {}).get("apollo") or {}).get("dreamview") or {}).get("record") or {}


def _region_from_dict(payload: Any) -> Optional[Dict[str, int]]:
    if not isinstance(payload, dict):
        return None
    try:
        region = {
            "x": int(payload.get("x")),
            "y": int(payload.get("y")),
            "width": int(payload.get("width")),
            "height": int(payload.get("height")),
        }
    except Exception:
        return None
    if region["width"] <= 0 or region["height"] <= 0:
        return None
    return region


def _manual_region(cfg: Dict[str, Any]) -> Optional[Dict[str, int]]:
    region = _region_from_dict(cfg.get("capture_region"))
    if region is not None:
        return region
    try:
        width = int(cfg.get("width") or 0)
        height = int(cfg.get("height") or 0)
        offset_x = int(cfg.get("offset_x") or 0)
        offset_y = int(cfg.get("offset_y") or 0)
    except Exception:
        return None
    if width <= 0 or height <= 0:
        return None
    return {"x": offset_x, "y": offset_y, "width": width, "height": height}


def _run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return proc.stdout


def _auto_detect_region(window_title: str) -> Optional[Dict[str, Any]]:
    xwininfo = shutil.which("xwininfo")
    if not xwininfo:
        return None
    xdotool = shutil.which("xdotool")
    wmctrl = shutil.which("wmctrl")
    ids: list[str] = []
    if xdotool:
        try:
            ids = [line.strip() for line in _run([xdotool, "search", "--onlyvisible", "--name", window_title]).splitlines() if line.strip()]
        except Exception:
            ids = []
    if not ids and wmctrl:
        try:
            for line in _run([wmctrl, "-lx"]).splitlines():
                if window_title.lower() not in line.lower():
                    continue
                token = line.strip().split()[0] if line.strip() else ""
                if token.startswith("0x"):
                    ids.append(token)
        except Exception:
            ids = []
    if not ids:
        try:
            for line in _run([xwininfo, "-root", "-tree"]).splitlines():
                if window_title.lower() not in line.lower():
                    continue
                token = line.strip().split()[0] if line.strip() else ""
                if token.startswith("0x"):
                    ids.append(token)
        except Exception:
            ids = []
    if not ids:
        return None
    win_id = ids[-1]
    text = _run([xwininfo, "-id", win_id])
    import re

    def _match(pattern: str) -> Optional[int]:
        match = re.search(pattern, text)
        return int(match.group(1)) if match else None

    region = {
        "x": _match(r"Absolute upper-left X:\s+(-?\d+)"),
        "y": _match(r"Absolute upper-left Y:\s+(-?\d+)"),
        "width": _match(r"Width:\s+(\d+)"),
        "height": _match(r"Height:\s+(\d+)"),
    }
    if any(value is None for value in region.values()):
        return None
    return {
        "window_id": win_id,
        "window_title_matched": window_title,
        "region": {key: int(value) for key, value in region.items()},
    }


def _load_cache(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Dreamview capture region and emit reusable config JSON.")
    parser.add_argument("--config", type=Path, help="Optional run config YAML to read algo.apollo.dreamview.record from.")
    parser.add_argument("--window-title", default="", help="Window title override for auto detection.")
    parser.add_argument("--cache-path", type=Path, default=DEFAULT_CACHE, help="Remembered-region cache JSON path.")
    parser.add_argument("--write", type=Path, help="Optional output JSON path.")
    args = parser.parse_args()

    cfg: Dict[str, Any] = {}
    if args.config:
        cfg = _record_cfg_from_profile(_load_yaml(args.config))
    window_title = args.window_title or str(cfg.get("window_title") or "Dreamview")

    manual = _manual_region(cfg)
    detected = _auto_detect_region(window_title)
    cached = _load_cache(args.cache_path)
    cached_region = _region_from_dict(cached.get("region"))

    recommended_region = None
    recommendation_source = ""
    if detected and detected.get("region"):
        recommended_region = detected["region"]
        recommendation_source = "auto-detected"
    elif manual:
        recommended_region = manual
        recommendation_source = "manual"
    elif cached_region:
        recommended_region = cached_region
        recommendation_source = "remembered"

    output = {
        "window_title": window_title,
        "manual_region": manual,
        "auto_detected": detected,
        "remembered_region": cached if cached else None,
        "recommended_region": recommended_region,
        "recommended_region_source": recommendation_source or None,
        "recommended_config": (
            {
                "use_fixed_region": True,
                "region_source": "manual",
                "capture_region": recommended_region,
            }
            if recommended_region
            else None
        ),
    }

    text = json.dumps(output, indent=2)
    if args.write:
        args.write.parent.mkdir(parents=True, exist_ok=True)
        args.write.write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
