from __future__ import annotations

import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

import yaml

from .schema import (
    BackendConfig,
    ConfigError,
    EgoConfig,
    RecordingConfig,
    RunConfig,
    ScenarioConfig,
    SensorRigConfig,
    SimConfig,
    TestbedConfig,
)


ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

BUILTIN_DEFAULTS: dict[str, Any] = {
    "run": {
        "id": "default",
        "max_ticks": 200,
        "fixed_dt_s": 0.05,
        "seed": 1,
        "output_root": "runs",
    },
    "sim": {
        "host": "localhost",
        "port": 2000,
        "town": "Town01",
        "synchronous": True,
        "timeout_s": 30.0,
    },
    "scenario": {
        "name": "",
        "params": {},
    },
    "ego": {
        "role_name": "hero",
        "blueprint": "vehicle.tesla.model3",
        "spawn": {},
    },
    "sensors": {
        "name": "minimal",
        "enabled": False,
        "specs": {},
    },
    "backend": {
        "name": "dummy",
        "params": {},
    },
    "recording": {
        "enabled": False,
        "artifacts": {},
    },
    "assist_ledger": {},
}

TOP_LEVEL_KEYS = set(BUILTIN_DEFAULTS) | {"output"}
STRICT_SECTION_KEYS = {
    "run": {"id", "max_ticks", "fixed_dt_s", "seed", "output_root"},
    "sim": {"host", "port", "town", "synchronous", "timeout_s"},
}
PARAM_SECTION_KEYS = {
    "scenario": ("params", {"name", "params"}),
    "backend": ("params", {"name", "params"}),
    "ego": ("spawn", {"role_name", "blueprint", "spawn"}),
    "sensors": ("specs", {"name", "enabled", "specs"}),
    "recording": ("artifacts", {"enabled", "artifacts"}),
}


def load_config(
    path: str | Path,
    *,
    local_override: str | Path | Mapping[str, Any] | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> TestbedConfig:
    """Load typed config v0.

    Load order is built-in defaults, main YAML file, optional local override, then
    CLI-style override dict. Strings support ${VAR} environment expansion. If VAR
    is not set, the original ${VAR} token is preserved so local templates remain
    loadable without host-specific secrets.

    Unknown top-level keys and unknown keys in strict sections are errors.
    Unknown keys in scenario/backend/ego/sensors/recording are folded into that
    section's params-like field.
    """

    config_path = Path(path)
    data = deepcopy(BUILTIN_DEFAULTS)
    deep_merge(data, _load_yaml_file(config_path))

    if local_override is not None:
        if isinstance(local_override, Mapping):
            local_data = deepcopy(dict(local_override))
        else:
            local_data = _load_yaml_file(Path(local_override))
        deep_merge(data, local_data)

    if overrides:
        deep_merge(data, deepcopy(dict(overrides)))

    data = expand_env_vars(data)
    data = _normalize_compat_fields(data)
    _validate_known_shape(data, config_path)
    cfg = _to_dataclass(data, config_path)
    cfg.validate_basic()
    return cfg


def deep_merge(base: dict[str, Any], patch: Mapping[str, Any]) -> dict[str, Any]:
    for key, value in patch.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def expand_env_vars(value: Any) -> Any:
    if isinstance(value, str):
        return ENV_PATTERN.sub(lambda m: os.environ.get(m.group(1), m.group(0)), value)
    if isinstance(value, list):
        return [expand_env_vars(item) for item in value]
    if isinstance(value, tuple):
        return tuple(expand_env_vars(item) for item in value)
    if isinstance(value, dict):
        return {key: expand_env_vars(item) for key, item in value.items()}
    return value


def _load_yaml_file(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    try:
        data = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"failed to parse YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: root must be a mapping")
    parent = data.get("extends")
    if parent:
        parent_path = Path(str(parent)).expanduser()
        if not parent_path.is_absolute():
            parent_path = path.parent / parent_path
        base = _load_yaml_file(parent_path)
        data = deep_merge(base, {key: value for key, value in data.items() if key != "extends"})
    return data


def _normalize_compat_fields(data: dict[str, Any]) -> dict[str, Any]:
    output = data.pop("output", None)
    if isinstance(output, Mapping) and "root" in output:
        data.setdefault("run", {})["output_root"] = output["root"]
    sim = data.get("sim")
    if isinstance(sim, Mapping) and "fixed_dt_s" in sim:
        data.setdefault("run", {})["fixed_dt_s"] = sim["fixed_dt_s"]
        if isinstance(sim, dict):
            sim.pop("fixed_dt_s", None)
    return data


def _validate_known_shape(data: Mapping[str, Any], source_path: Path) -> None:
    for key in data:
        if key not in TOP_LEVEL_KEYS:
            raise ConfigError(f"{source_path}: unknown top-level key '{key}'")

    for section, allowed in STRICT_SECTION_KEYS.items():
        section_data = data.get(section, {})
        if not isinstance(section_data, Mapping):
            raise ConfigError(f"{source_path}: {section} must be a mapping")
        for key in section_data:
            if key not in allowed:
                raise ConfigError(f"{source_path}: unknown field {section}.{key}")

    for section in PARAM_SECTION_KEYS:
        section_data = data.get(section, {})
        if not isinstance(section_data, Mapping):
            raise ConfigError(f"{source_path}: {section} must be a mapping")


def _fold_params(section_data: Mapping[str, Any], params_key: str, allowed: set[str]) -> dict[str, Any]:
    typed = {key: deepcopy(value) for key, value in section_data.items() if key in allowed}
    params = dict(typed.get(params_key) or {})
    for key, value in section_data.items():
        if key not in allowed:
            params[key] = deepcopy(value)
    typed[params_key] = params
    return typed


def _to_dataclass(data: Mapping[str, Any], source_path: Path) -> TestbedConfig:
    scenario = _fold_params(data.get("scenario", {}), "params", PARAM_SECTION_KEYS["scenario"][1])
    backend = _fold_params(data.get("backend", {}), "params", PARAM_SECTION_KEYS["backend"][1])
    ego = _fold_params(data.get("ego", {}), "spawn", PARAM_SECTION_KEYS["ego"][1])
    sensors = _fold_params(data.get("sensors", {}), "specs", PARAM_SECTION_KEYS["sensors"][1])
    recording = _fold_params(data.get("recording", {}), "artifacts", PARAM_SECTION_KEYS["recording"][1])

    return TestbedConfig(
        run=RunConfig(**dict(data["run"])),
        sim=SimConfig(**dict(data["sim"])),
        scenario=ScenarioConfig(**scenario),
        ego=EgoConfig(**ego),
        sensors=SensorRigConfig(**sensors),
        backend=BackendConfig(**backend),
        recording=RecordingConfig(**recording),
        assist_ledger=dict(data.get("assist_ledger") or {}),
        source_path=source_path,
    )
