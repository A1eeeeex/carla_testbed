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
