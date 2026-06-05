from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict


@dataclass(frozen=True)
class RunConfig:
    id: str = "default"
    max_ticks: int = 200
    fixed_dt_s: float = 0.05
    seed: int = 1
    output_root: str = "runs"


@dataclass(frozen=True)
class SimConfig:
    host: str = "localhost"
    port: int = 2000
    town: str = "Town01"
    synchronous: bool = True
    timeout_s: float = 30.0


@dataclass(frozen=True)
class ScenarioConfig:
    name: str = ""
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EgoConfig:
    role_name: str = "hero"
    blueprint: str = "vehicle.tesla.model3"
    spawn: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SensorRigConfig:
    name: str = "minimal"
    enabled: bool = False
    specs: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BackendConfig:
    name: str = "dummy"
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecordingConfig:
    enabled: bool = False
    artifacts: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TestbedConfig:
    run: RunConfig = field(default_factory=RunConfig)
    sim: SimConfig = field(default_factory=SimConfig)
    scenario: ScenarioConfig = field(default_factory=ScenarioConfig)
    ego: EgoConfig = field(default_factory=EgoConfig)
    sensors: SensorRigConfig = field(default_factory=SensorRigConfig)
    backend: BackendConfig = field(default_factory=BackendConfig)
    recording: RecordingConfig = field(default_factory=RecordingConfig)
    assist_ledger: Dict[str, Any] = field(default_factory=dict)
    source_path: Path | None = None

    def validate_basic(self) -> None:
        errors = []
        if self.run.max_ticks <= 0:
            errors.append("run.max_ticks must be > 0")
        if self.run.fixed_dt_s <= 0:
            errors.append("run.fixed_dt_s must be > 0")
        if not isinstance(self.sim.port, int):
            errors.append("sim.port must be an int")
        if not str(self.scenario.name or "").strip():
            errors.append("scenario.name must be non-empty")
        if not str(self.backend.name or "").strip():
            errors.append("backend.name must be non-empty")
        if errors:
            origin = f" in {self.source_path}" if self.source_path else ""
            raise ConfigValidationError("; ".join(errors) + origin)


class ConfigError(ValueError):
    """Base error for typed config loading and validation."""


class ConfigValidationError(ConfigError):
    """Raised when a config value violates the v0 typed config contract."""
