# Testing

The default test path must be CI-friendly: it should not require a live CARLA
server, Apollo Docker, CyberRT, ROS2 daemon, GPU rendering, or Dreamview.

## Default CI Command

Use this command for normal pull-request checks:

```bash
python -m pytest -m "not carla and not apollo and not autoware and not integration" -q
```

This includes unit tests, config/schema tests, contract tests, mock backend
tests, artifact writers, and runtime import guards.

The default pytest collection also includes the selected structure regression
file `tools/test_first_wave_structure.py`. Other historical `tools/test_*.py`
files remain outside the default test path until they are either migrated under
`tests/`, marked as local/integration, or retired. This prevents hidden operator
surface regressions without suddenly promoting all legacy tools tests to CI.

Use the project conda environment when running locally on the development
machine:

```bash
conda run -n carla16 python -m pytest -m "not carla and not apollo and not autoware and not integration" -q
```

## Test Categories

### 1. No-CARLA Unit Tests

These tests exercise pure Python contracts and helpers:

- `carla_testbed/config`
- `carla_testbed/core`
- `carla_testbed/contracts`
- `carla_testbed/control` contract helpers
- `carla_testbed/evaluation`
- `carla_testbed/record`
- `carla_testbed/adapters/base.py`
- Apollo mock adapter tests

They should run in CI without CARLA or Apollo installed.

Useful focused commands:

```bash
python -m pytest tests/unit/config tests/unit/contracts tests/unit/core -q
python -m pytest tests/unit/control tests/unit/recording tests/unit/evaluation -q
```

### 2. Mock CARLA Unit Tests

Some tests use fake vehicle/sensor objects to validate behavior such as control
application or sensor synchronization. These are still regular unit tests and
should not require a CARLA server.

Examples:

```bash
python -m pytest tests/unit/control/test_control_applicator.py -q
python -m pytest tests/unit/sensors -q
```

### 3. Real CARLA Smoke Tests

Tests or commands that need a live simulator must be marked `carla` or kept out
of the default CI command. They are for local validation of spawn, tick, sensor,
recording, and short closed-loop behavior.

Example local shape:

```bash
python -m pytest -m carla -q
```

Run only after CARLA is installed and reachable.

CLI smoke is different: it is a no-runtime config/artifact check and should
stay safe for CI:

```bash
python -m carla_testbed smoke --config configs/examples/smoke.yaml
```

### 4. Apollo Bridge Contract Tests

Apollo adapter contract tests should use mock backend or import-only CyberRT
skeleton checks by default:

```bash
python -m pytest tests/unit/adapters/apollo -q
```

These tests prove mapping and boundaries. They do not prove real Apollo routing,
planning, or control materialization.

Real Apollo runtime checks must be marked `apollo` or run manually outside the
default CI path.

### 5. Autoware Runtime Tests

Autoware tests that require ROS2 graph state, Autoware Docker containers, RViz2,
rosbag2, or a running bridge must be marked `autoware` or `integration`.
Offline Autoware config/schema/recorder command-construction tests can remain
in the default CI set.

Example local shape:

```bash
python -m pytest -m autoware -q
```

### 6. Local Heavy Integration Tests

Tests that require Apollo/CyberRT, ROS2 graph state, Docker containers, or
long-running CARLA sessions must be marked `apollo` or `integration`, and should
not run in default CI.

Suggested local command shape:

```bash
python -m pytest -m "apollo or integration" -q
```

Only run this on a machine with the full runtime stack configured.

## Markers

- `unit`: fast tests without live external services.
- `smoke`: lightweight executable smoke checks.
- `carla`: requires CARLA simulator or CARLA Python runtime.
- `apollo`: requires Apollo/CyberRT runtime.
- `autoware`: requires Autoware/ROS2 runtime.
- `integration`: requires external services, long-running processes, or full
  simulator/backend orchestration.
- `local`: local-machine checks that are not expected to run in CI.

`tests/conftest.py` automatically marks files under `tests/unit/` as `unit`.

## Runtime Import Guard

`tests/unit/test_no_runtime_imports.py` prevents runtime stack imports from
leaking into core boundaries. It currently checks:

- `carla_testbed/core`
- `carla_testbed/contracts`
- `carla_testbed/adapters/base.py`

These modules must not import CARLA, ROS2, CyberRT, or Apollo protobufs.

## Ruff

Ruff is configured conservatively in `pyproject.toml` with basic `E` and `F`
rules. It is useful for new code and focused cleanups, but should not be used as
a reason to rewrite unrelated legacy files in a large sweep.

Run if available:

```bash
python -m ruff check .
```

If `ruff` is not installed in the active environment, do not add it as a runtime
dependency just to satisfy this project. Treat it as a developer tool.
