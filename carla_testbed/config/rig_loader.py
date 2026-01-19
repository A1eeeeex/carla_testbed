from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

from carla_testbed.sensors.specs import SensorSpec


def _load_yaml_or_json(path: Path) -> Dict:
    text = path.read_text()
    if path.suffix.lower() in [".yaml", ".yml"]:
        return yaml.safe_load(text)
    return json.loads(text)


def load_rig_preset(name: str, preset_dir: Path) -> Dict:
    path = preset_dir / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"preset {name} not found at {path}")
    return _load_yaml_or_json(path)


def load_rig_file(path: str) -> Dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"rig file not found: {path}")
    return _load_yaml_or_json(p)


def _parse_scalar(val: str):
    if val.lower() in ["true", "false"]:
        return val.lower() == "true"
    try:
        if "." in val:
            return float(val)
        return int(val)
    except Exception:
        return val


def apply_overrides(rig: Dict, overrides: List[str]) -> Dict:
    out = deepcopy(rig)
    for ov in overrides:
        if "=" not in ov:
            continue
        path, raw = ov.split("=", 1)
        val = _parse_scalar(raw.strip())
        keys = path.strip().split(".")
        cur = out
        for k in keys[:-1]:
            if k not in cur or not isinstance(cur[k], dict):
                cur[k] = {}
            cur = cur[k]
        cur[keys[-1]] = val
    return out


def rig_to_specs(rig: Dict) -> Tuple[List[SensorSpec], Dict]:
    sensors = []
    for s in rig.get("sensors", []):
        if not s.get("enabled", True):
            continue
        spec = SensorSpec(
            sensor_id=s["id"],
            sensor_type=s["type"],
            blueprint=s["blueprint"],
            enabled=s.get("enabled", True),
            sensor_tick=s.get("sensor_tick", 0.05),
            transform=s.get("transform", {}),
            attributes={k: str(v) for k, v in (s.get("attributes") or {}).items()},
        )
        sensors.append(spec)
    events = rig.get("events", {"collision": True, "lane_invasion": True})
    # enforce events on unless explicitly disabled in rig file
    events.setdefault("collision", True)
    events.setdefault("lane_invasion", True)
    return sensors, events


def dump_rig(run_dir: Path, rig_raw: Dict, rig_final: Dict, specs: List[SensorSpec], meta_path: Path = None) -> None:
    cfg_dir = run_dir / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    # rig raw: only keep rig info + reference to legacy meta
    raw_out = {
        "rig_name": rig_raw.get("name"),
        "rig_version": rig_raw.get("version"),
        "meta_ref": str(meta_path) if meta_path else None,
    }
    (cfg_dir / "sensors_rig_raw.yaml").write_text(yaml.safe_dump(raw_out, sort_keys=False))
    (cfg_dir / "sensors_rig_final.yaml").write_text(yaml.safe_dump(rig_final, sort_keys=False))
    specs_out = []
    for s in specs:
        specs_out.append(
            {
                "id": s.sensor_id,
                "type": s.sensor_type,
                "blueprint": s.blueprint,
                "enabled": s.enabled,
                "sensor_tick": s.sensor_tick,
                "transform": s.transform,
                "attributes": s.attributes,
            }
        )
    (cfg_dir / "sensors_expanded.json").write_text(json.dumps(specs_out, indent=2))
