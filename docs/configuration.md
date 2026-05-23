# Configuration

This document describes the current configuration layout and the v0 typed
loader contract. It complements `configs/README.md`; if they diverge, update
both instead of letting one become stale.

## Configuration Priority

New code should follow this precedence order:

1. Built-in defaults in `carla_testbed/config/loader.py`.
2. Project config, for example `configs/project.yaml`.
3. Profile config, for example `configs/profiles/*.yaml`.
4. Rig config, for example `configs/rigs/*.yaml`.
5. Local override, for example `configs/local/*.yaml` or environment variables.
6. CLI overrides, for example `--override run.max_ticks=50`.

Legacy runners under `configs/io/`, `tbio/`, or `tools/` may still have their
own loading rules. Treat the order above as the target contract for new
platform work.

## Directory Roles

| Path | Role | Status |
| --- | --- | --- |
| `configs/examples/` | Small canonical examples for docs and smoke tests. | Supported for typed v0 configs. |
| `configs/profiles/` | Reusable scenario/backend profiles. | Supported as the profile library fills in. |
| `configs/rigs/` | Sensor rig presets. | Supported. |
| `configs/local/` | Machine-local override templates and private local configs. | Supported; commit placeholders only. |
| `configs/io/` | Historical run profiles, Apollo/Town01 experiments, maps, and contracts. | Transitional / legacy compatibility. |

The current canonical typed example is:

```bash
python -m carla_testbed config-validate configs/examples/smoke.yaml
```

## Typed Config v0

`carla_testbed/config/schema.py` defines the current core dataclasses:

- `RunConfig`: `id`, `max_ticks`, `fixed_dt_s`, `seed`, `output_root`.
- `SimConfig`: `host`, `port`, `town`, `synchronous`, `timeout_s`.
- `ScenarioConfig`: `name`, `params`.
- `EgoConfig`: `role_name`, `blueprint`, `spawn`.
- `SensorRigConfig`: `name`, `enabled`, `specs`.
- `BackendConfig`: `name`, `params`.
- `RecordingConfig`: `enabled`, `artifacts`.
- `TestbedConfig`: the top-level bundle.

`load_config(...)` merges built-in defaults, the main YAML file, an optional
local override, and optional CLI-style overrides. It validates:

- `run.max_ticks > 0`
- `run.fixed_dt_s > 0`
- `sim.port` is an integer
- `scenario.name` is non-empty
- `backend.name` is non-empty

Unknown top-level keys and unknown keys in strict sections are errors. Unknown
fields in `scenario`, `backend`, `ego`, `sensors`, and `recording` are folded
into that section's params-like field so experiments can carry extra metadata
without changing the core schema every time.

## Environment Placeholders

Tracked config may use placeholders such as:

```text
${CARLA_ROOT}
${APOLLO_ROOT}
${APOLLO_MAP_ROOT}
${APOLLO_DOCKER_CONTAINER}
${AUTOWARE_MAP_ROOT}
${CARLA16_PYTHON}
${TESTBED_OUTPUT_ROOT}
```

The typed loader expands `${VAR}` from the process environment when `VAR` is
set. If `VAR` is not set, the original placeholder text is preserved. This lets
templates remain loadable without leaking host-specific paths.

## Local Override Policy

Use `configs/local/local.example.yaml` as the template for private local
configuration. Copy it to an ignored/private file before adding real machine
values.

Typed v0 local overrides should use typed top-level sections such as `run`,
`sim`, `scenario`, `ego`, `sensors`, `backend`, and `recording`. Broader legacy
keys such as Apollo roots or Docker container names may still be consumed by
transitional launchers and older configs, but they are not automatically part
of the strict typed v0 schema unless they are placed in a params-like section.

Local override is the right place for:

- CARLA host and port.
- `CARLA_ROOT`.
- `APOLLO_ROOT`.
- `APOLLO_MAP_ROOT`.
- Apollo Docker container name.
- local Python executable paths for compatibility launchers.
- local output roots.

Tracked YAML must not contain real machine-bound absolute paths such as Linux
home directories, macOS user directories, Windows user directories, mounted
data roots, or long-lived temporary output roots.

Use the tracked config path scanner before committing config changes:

```bash
python -m pytest tests/unit/config/test_no_hardcoded_paths.py -q
```

## Legacy Configs

`configs/io/examples/` remains important for reproducibility and current
Apollo/Town01 experiments. Do not delete those profiles as part of typed config
cleanup. Also do not add new canonical platform shape there unless the task is
explicitly a legacy compatibility change.

For new docs, smoke checks, or platform examples, prefer `configs/examples/`
and keep the file small. Apollo-specific bridge tuning, Town01 route-health
experiments, and historical profiles should stay clearly marked as transitional
until they are promoted into a typed profile.
