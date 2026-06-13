from __future__ import annotations

from pathlib import Path

import pytest

from carla_testbed.config.loader import load_config
from carla_testbed.config.schema import ConfigError, ConfigValidationError


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_loads_smoke_config() -> None:
    cfg = load_config(REPO_ROOT / "configs" / "examples" / "smoke.yaml")

    assert cfg.run.id == "smoke"
    assert cfg.run.max_ticks == 200
    assert cfg.run.fixed_dt_s == 0.05
    assert cfg.run.output_root == "runs"
    assert cfg.sim.host == "localhost"
    assert cfg.sim.port == 2000
    assert cfg.sim.town == "Town01"
    assert cfg.scenario.name == "follow_stop"
    assert cfg.backend.name == "dummy"


def test_cli_override_can_override_max_ticks() -> None:
    cfg = load_config(
        REPO_ROOT / "configs" / "examples" / "smoke.yaml",
        overrides={"run": {"max_ticks": 7}},
    )

    assert cfg.run.max_ticks == 7


def test_missing_required_field_has_clear_error(tmp_path: Path) -> None:
    config_path = tmp_path / "missing_required.yaml"
    config_path.write_text(
        """
run:
  id: missing-required
backend:
  name: dummy
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigValidationError, match=r"scenario\.name.*missing_required\.yaml"):
        load_config(config_path)


def test_unknown_top_level_field_is_clear_error(tmp_path: Path) -> None:
    config_path = tmp_path / "unknown_top_level.yaml"
    config_path.write_text(
        """
run:
  id: unknown-top-level
scenario:
  name: follow_stop
backend:
  name: dummy
surprise:
  enabled: true
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match=r"unknown top-level key 'surprise'"):
        load_config(config_path)


def test_unknown_backend_fields_fold_into_params(tmp_path: Path) -> None:
    config_path = tmp_path / "backend_params.yaml"
    config_path.write_text(
        """
scenario:
  name: follow_stop
backend:
  name: dummy
  control_topic: /tb/ego/control_cmd
""",
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.backend.name == "dummy"
    assert cfg.backend.params == {"control_topic": "/tb/ego/control_cmd"}


def test_legacy_record_io_algo_aliases_are_preserved_for_typed_claim_configs(tmp_path: Path) -> None:
    config_path = tmp_path / "legacy_aliases.yaml"
    config_path.write_text(
        """
run:
  id: claim-probe
  claim_profile: true
  ticks: 12
  fixed_delta_seconds: 0.1
  fail_strategy: log_and_continue
scenario:
  driver: carla_town01_route_health
backend:
  name: apollo_cyberrt
record:
  sensors:
    enable: false
io:
  mode: ros2_native
algo:
  stack: apollo
paths:
  apollo_root: /opt/apollo/neo
carla:
  host: 127.0.0.1
runtime:
  mode: online
logging:
  level: info
""",
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.run.claim_profile is True
    assert cfg.run.max_ticks == 12
    assert cfg.run.fixed_dt_s == 0.1
    assert cfg.scenario.name == "carla_town01_route_health"
    assert cfg.backend.name == "apollo_cyberrt"
    assert cfg.recording.artifacts["legacy_record"]["sensors"]["enable"] is False
    assert cfg.backend.params["legacy_io"]["mode"] == "ros2_native"
    assert cfg.backend.params["legacy_algo"]["stack"] == "apollo"
    assert cfg.backend.params["legacy_paths"]["apollo_root"] == "/opt/apollo/neo"
    assert cfg.backend.params["legacy_carla"]["host"] == "127.0.0.1"
    assert cfg.backend.params["legacy_runtime"]["mode"] == "online"
    assert cfg.backend.params["legacy_logging"]["level"] == "info"
    assert cfg.backend.params["legacy_run"]["fail_strategy"] == "log_and_continue"
    assert any(item["from"] == "record" for item in cfg.config_aliases_used)


def test_top_level_assist_ledger_is_a_known_contract_section(tmp_path: Path) -> None:
    config_path = tmp_path / "assist_ledger.yaml"
    config_path.write_text(
        """
scenario:
  name: follow_stop
backend:
  name: dummy
assist_ledger:
  schema_version: assist_ledger.v1
  active_assists: []
  assist_confidence: explicit
  source_artifact: config
  can_claim_unassisted_natural_driving: true
""",
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.assist_ledger["schema_version"] == "assist_ledger.v1"
    assert cfg.assist_ledger["active_assists"] == []
    assert cfg.assist_ledger["assist_confidence"] == "explicit"


def test_env_var_expansion_preserves_unset_vars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CARLA_TESTBED_TMP_OUTPUT", "runs/from-env")
    monkeypatch.delenv("CARLA_TESTBED_UNSET_OUTPUT", raising=False)
    config_path = tmp_path / "env.yaml"
    config_path.write_text(
        """
run:
  output_root: ${CARLA_TESTBED_TMP_OUTPUT}/${CARLA_TESTBED_UNSET_OUTPUT}
scenario:
  name: follow_stop
backend:
  name: dummy
""",
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.run.output_root == "runs/from-env/${CARLA_TESTBED_UNSET_OUTPUT}"


def test_town01_materialization_probe_starts_carla_for_online_reproducibility() -> None:
    cfg = load_config(
        REPO_ROOT
        / "configs"
        / "io"
        / "examples"
        / "town01_apollo_route_materialization_probe.yaml"
    )

    runtime_carla = ((cfg.backend.params.get("legacy_runtime") or {}).get("carla") or {})

    assert runtime_carla.get("start") is True
    assert runtime_carla.get("extra_args") == "--ros2"
