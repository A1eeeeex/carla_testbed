# Tools Boundary

`tools/` is an operator surface, not a canonical architecture layer. Scripts in
this directory may parse CLI arguments, compose local commands, copy artifacts,
or delegate to package modules. They should not become the place where new
runner, analyzer, adapter, calibration, or acceptance logic lives.

## Allowed Responsibilities

- CLI parsing and help text.
- Local orchestration for one-off validation, demos, or regressions.
- Thin delegation into `carla_testbed.analysis`, `carla_testbed.experiments`,
  `carla_testbed.apollo`, `carla_testbed.autoware`, `carla_testbed.record`, or
  `carla_testbed.calibration`.
- Formatting command output or writing small wrapper manifests.

## Disallowed New Responsibilities

- New scenario gate logic.
- New route-health, control, calibration, A/B, or natural-driving analyzers.
- New CARLA, CyberRT, ROS2, Apollo, or Autoware runtime abstractions.
- Large reusable functions or classes that should be package modules.
- New public APIs.

## Legacy Allowlist

Some historical `tools/run_*.py` files are still large because they predate the
current package boundaries. They are tracked in
`configs/tools_boundary_allowlist.yaml`. Every allowlisted entry must say why it
exists, where it should eventually migrate, and `allowed_to_grow: false`.

Allowlisted scripts are compatibility debt, not examples for new work. They
must carry a `LEGACY` or `OPERATIONAL HELPER` header and should only receive
small compatibility or safety fixes. New platform behavior should be moved into
package modules first, then called from a thin wrapper.

## Test Guard

`tests/test_tools_boundary.py` enforces the boundary:

- non-allowlisted `tools/run_*.py` files over 300 lines fail;
- non-allowlisted run tools with direct CARLA/CyberRT imports, classes,
  dataclasses, or large non-`main` functions fail;
- medium-sized non-allowlisted run tools should delegate to `carla_testbed`
  package modules;
- allowlisted scripts must have boundary headers.

This intentionally does not delete old tools. It prevents the next tool from
quietly becoming the next runner.
