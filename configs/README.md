# configs/

Configuration root for the CARLA testbed.

This directory is being cleaned up in stages. New configuration work should use the lightweight layout below, while the older `configs/io/examples/` profiles remain available as transitional compatibility profiles.

## Intended Layout

| Path | Role | Status |
| --- | --- | --- |
| `project.yaml` | Project-wide defaults that are safe to commit. | Supported |
| `examples/` | Small canonical examples for smoke tests and docs. | Supported |
| `profiles/` | Reusable scenario/backend profiles. | Planned / supported as it fills in |
| `rigs/` | Sensor rig presets. | Supported |
| `local/` | Machine-local override templates. | Supported, placeholders only |
| `io/` | Older run profiles, contracts, and maps. | Transitional / legacy compatibility |

## Config Precedence

When the typed config loader is introduced, the intended precedence is:

1. Built-in defaults in code.
2. Project config, for example `configs/project.yaml`.
3. Profile config, for example `configs/profiles/*.yaml`.
4. Rig config, for example `configs/rigs/*.yaml`.
5. Local override, for example `configs/local/*.yaml` or environment variables.
6. CLI overrides, for example `--override key=value`.

Current legacy runners may still load some paths differently. Treat this list as the target contract for new code and docs.

## Machine-Local Values

Tracked config files must not contain machine-bound absolute paths such as Linux home directories, macOS user directories, or Windows user directories. Keep real local paths out of Git.

Use local overrides for:

- CARLA host and port.
- `CARLA_ROOT`.
- `APOLLO_ROOT`.
- `APOLLO_MAP_ROOT`.
- `AUTOWARE_MAP_ROOT`.
- `CARLA16_PYTHON` or another local Python executable path used by compatibility launchers.
- Docker container name.
- Local output root.

Start from:

```text
configs/local/local.example.yaml
```

Copy it to a private filename such as `configs/local/local.yaml` or `configs/local/<hostname>.yaml`, then fill in real paths locally. Do not commit real machine paths.

Tracked YAML may use environment placeholders such as:

```text
${CARLA_ROOT}
${APOLLO_ROOT}
${APOLLO_MAP_ROOT}
${APOLLO_DOCKER_CONTAINER}
${AUTOWARE_MAP_ROOT}
${CARLA16_PYTHON}
${TESTBED_OUTPUT_ROOT}
```

These placeholders must be resolved by the typed config loader, shell environment, local override, or a CLI override before a runtime path is used. If a legacy profile has a field whose path semantics are unclear, keep the field but move any real machine value to local override rather than committing it.

## Canonical Examples

The new example surface is intentionally small:

- `configs/examples/smoke.yaml`

It is a shape example for future canonical configs. It does not replace the older Apollo or follow-stop profiles yet.

## Transitional Profiles

`configs/io/examples/` contains many historical follow-stop, Apollo, Town01, calibration, and demo profiles. They are kept for reproducibility and ongoing experiments, but they are not the target location for new platform configuration.

Transitional profiles may still contain env placeholders for local paths. Do not replace those placeholders with real personal home-directory paths, mounted data roots, or long-lived temporary-directory roots in tracked config.
