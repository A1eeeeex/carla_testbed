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
        "claim_profile": False,
        "materialization_probe": False,
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
    "run": {
        "id",
        "max_ticks",
        "fixed_dt_s",
        "seed",
        "output_root",
        "claim_profile",
        "materialization_probe",
    },
    "sim": {"host", "port", "town", "synchronous", "timeout_s"},
}
PARAM_SECTION_KEYS = {
    "scenario": ("params", {"name", "params"}),
    "backend": ("params", {"name", "params"}),
    "ego": ("spawn", {"role_name", "blueprint", "spawn"}),
    "sensors": ("specs", {"name", "enabled", "specs"}),
    "recording": ("artifacts", {"enabled", "artifacts"}),
}
LEGACY_ROOT_KEYS = ("io", "algo", "paths", "carla", "runtime", "logging", "reports", "checks", "profiles")


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
    aliases_used = list(data.get("__config_aliases_used__", []))
    output = data.pop("output", None)
    if isinstance(output, Mapping) and "root" in output:
        data.setdefault("run", {})["output_root"] = output["root"]
        aliases_used.append({"from": "output.root", "to": "run.output_root"})
    record = data.pop("record", None)
    if isinstance(record, Mapping):
        recording = data.setdefault("recording", {})
        if isinstance(recording, dict):
            artifacts = recording.setdefault("artifacts", {})
            if isinstance(artifacts, dict):
                artifacts.setdefault("legacy_record", deepcopy(dict(record)))
                artifacts.setdefault("config_aliases_used", []).append(
                    {"from": "record", "to": "recording.artifacts.legacy_record"}
                )
        aliases_used.append({"from": "record", "to": "recording.artifacts.legacy_record"})
    for legacy_key in LEGACY_ROOT_KEYS:
        legacy_section = data.pop(legacy_key, None)
        if isinstance(legacy_section, Mapping):
            backend = data.setdefault("backend", {})
            if isinstance(backend, dict):
                if (
                    legacy_key == "algo"
                    and backend.get("name") == BUILTIN_DEFAULTS["backend"]["name"]
                ):
                    stack = str(legacy_section.get("stack") or "").strip()
                    if stack == "apollo":
                        backend["name"] = "apollo_cyberrt"
                        aliases_used.append({"from": "algo.stack", "to": "backend.name"})
                    elif stack == "autoware":
                        backend["name"] = "autoware_ros2"
                        aliases_used.append({"from": "algo.stack", "to": "backend.name"})
                params = backend.setdefault("params", {})
                if isinstance(params, dict):
                    params.setdefault(f"legacy_{legacy_key}", deepcopy(dict(legacy_section)))
                    aliases_used.append(
                        {"from": legacy_key, "to": f"backend.params.legacy_{legacy_key}"}
                    )
    scenario = data.get("scenario")
    if isinstance(scenario, dict) and not scenario.get("name"):
        for legacy_name_key in ("name", "driver", "scenario_id", "id"):
            value = scenario.get(legacy_name_key)
            if value not in {None, ""}:
                scenario["name"] = str(value)
                if legacy_name_key != "name":
                    aliases_used.append({"from": f"scenario.{legacy_name_key}", "to": "scenario.name"})
                break
    run = data.get("run")
    if isinstance(run, dict):
        legacy_ticks = run.pop("ticks", None)
        if legacy_ticks is not None and run.get("max_ticks") == BUILTIN_DEFAULTS["run"]["max_ticks"]:
            run["max_ticks"] = legacy_ticks
            aliases_used.append({"from": "run.ticks", "to": "run.max_ticks"})
        legacy_fixed_delta = run.pop("fixed_delta_seconds", None)
        if legacy_fixed_delta is not None and run.get("fixed_dt_s") == BUILTIN_DEFAULTS["run"]["fixed_dt_s"]:
            run["fixed_dt_s"] = legacy_fixed_delta
            aliases_used.append({"from": "run.fixed_delta_seconds", "to": "run.fixed_dt_s"})
        allowed_run = STRICT_SECTION_KEYS["run"]
        legacy_run_params = {
            key: deepcopy(value)
            for key, value in list(run.items())
            if key not in allowed_run
        }
        if legacy_run_params:
            for key in legacy_run_params:
                run.pop(key, None)
            backend = data.setdefault("backend", {})
            if isinstance(backend, dict):
                params = backend.setdefault("params", {})
                if isinstance(params, dict):
                    params.setdefault("legacy_run", {}).update(legacy_run_params)
                    aliases_used.append({"from": "run.<legacy_fields>", "to": "backend.params.legacy_run"})
    sim = data.get("sim")
    if isinstance(sim, Mapping) and "fixed_dt_s" in sim:
        data.setdefault("run", {})["fixed_dt_s"] = sim["fixed_dt_s"]
        if isinstance(sim, dict):
            sim.pop("fixed_dt_s", None)
        aliases_used.append({"from": "sim.fixed_dt_s", "to": "run.fixed_dt_s"})
    data["__config_aliases_used__"] = aliases_used
    return data


def _validate_known_shape(data: Mapping[str, Any], source_path: Path) -> None:
    for key in data:
        if key == "__config_aliases_used__":
            continue
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
        config_aliases_used=[dict(item) for item in data.get("__config_aliases_used__") or []],
        source_path=source_path,
    )
