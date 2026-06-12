# Architecture

This document is the current developer map for the CARLA testbed. It describes
the intended engineering boundaries after the v0 cleanup work. Durable runtime
truths, interface contracts, and verified findings still live under
`reference_pack/reference/`.

## Current Goal

The repository is a CARLA-based simulation harness for autonomous driving
experiments, currently prioritizing a CARLA + Apollo ground-truth MVP closed
loop. The immediate engineering target is not a polished demo; it is a
repeatable platform where CARLA state, scenario intent, backend inputs/outputs,
control application, artifacts, and metrics have explicit boundaries.

Current support is intentionally layered:

- Core platform: config loading, runner/harness, lifecycle, scenarios, sensors,
  control contracts, recording, metrics, and run artifacts.
- Baseline/demo: follow-stop remains a legacy baseline for smoke checks and
  demonstrations.
- External backend: ROS2/`tbio` is transitional; Apollo/CyberRT has an MVP mock
  backend and a lazy CyberRT skeleton, but real Apollo closed-loop promotion
  still requires local runtime evidence.

Autoware remains legacy experimental support and is not the current equal
priority target.

## Directory Responsibilities

| Path | Responsibility | Runtime dependency rule |
| --- | --- | --- |
| `carla_testbed/core/` | Run context, lifecycle cleanup, frame/time helpers. | No CARLA, ROS2, CyberRT, Apollo protobuf. |
| `carla_testbed/contracts/` | Runtime-neutral state, truth, geometry, and control models. | No CARLA, ROS2, CyberRT, Apollo protobuf. |
| `carla_testbed/config/` | Typed config v0 schema and lightweight loader. | No simulator or AD-stack runtime import. |
| `carla_testbed/runner/` | Harness orchestration, staged tick loop, hooks, cleanup integration. | May coordinate runtime pieces, but should depend on neutral contracts where possible. |
| `carla_testbed/scenarios/` | Scenario intent, registry, actor/route metadata. | No backend startup, controller behavior, or acceptance logic. |
| `carla_testbed/sim/` | CARLA-specific simulation helpers. | CARLA-specific code belongs here, not in contracts. |
| `carla_testbed/sensors/` | Sensor rig lifecycle and synchronization stats. | Sensor runtime details stay here; ROS2/CyberRT conversion belongs in adapters. |
| `carla_testbed/control/` | Controller contract, dummy controller, legacy adapter, control applicator. | New controller interface uses contracts; CARLA application is isolated in applicator code. |
| `carla_testbed/record/` | Run artifact writers, timeseries, recording utilities. | Recording should consume run state, not own scenario/backend logic. |
| `carla_testbed/evaluation/` | Minimal comparable run metrics. | No simulator/runtime startup. |
| `carla_testbed/adapters/` | Backend contract and stack-specific adapter boundaries. | Runtime imports are kept behind adapter implementations. |
| `tbio/` | Transitional backend implementation layer. | Still used by legacy/current runnable paths. |
| `io/` | Deprecated compatibility wrapper surface. | Do not add new platform logic here. |
| `examples/` | Lightweight examples and legacy demo wrappers. | Examples should call platform APIs, not become platform code. |
| `tools/` | Operational helpers, regressions, batch runners, analysis scripts. | Not a canonical architecture layer. |

See `docs/tools_boundary.md` for the enforced rule: new reusable logic belongs
in `carla_testbed.*` package modules, while `tools/run_*.py` remains a thin
CLI/orchestration surface. Historical large scripts are listed in
`configs/tools_boundary_allowlist.yaml` and are not allowed to grow.

## Dependency Direction

The intended direction is:

```text
config
  -> runner
  -> scenario / sim / sensors / control / record / evaluation / adapters
  -> external runtimes
```

The dependency boundary that matters most:

```text
core + contracts
  must not import
CARLA / ROS2 / CyberRT / Apollo protobuf / Autoware messages
```

Adapters convert between core contracts and external runtimes. For example,
Apollo protobuf conversion belongs under `carla_testbed/adapters/apollo/` or an
explicit experimental bridge area, not in `carla_testbed/contracts/`.

## Runner Shape

The runner is being staged around a small set of responsibilities:

1. Resolve config and create a run context.
2. Set up scenario, sensors, backend, recorders, and lifecycle cleanup.
3. Execute a predictable tick loop.
4. Publish or collect state through hooks/adapters.
5. Compute or poll control.
6. Apply control through a single control applicator boundary.
7. Record artifacts and metrics.
8. Clean up resources in reverse registration order.

`carla_testbed/runner/hooks.py` defines the v0 hook contract. Hooks are for
observing or integrating a run stage; they should not become hidden ownership
of world ticking or resource lifecycle.

## Scenario Boundary

`carla_testbed/scenarios/` describes scenario intent and actor setup. The v0
interface contains:

- `ScenarioSpec`: name, town, ego spawn, actor params, route/goal metadata,
  duration, max ticks, and metadata.
- `ScenarioState`: ego actor id, actor ids, route/goal metadata, status, and
  scenario-local metadata.
- `ScenarioRunner`: `setup(world, context, config)`, `tick(frame_context)`, and
  `teardown()`.
- `ScenarioRegistry`: name-based lookup for scenario builders.

`follow_stop` is now a baseline scenario, not the identity of the whole
platform. The large historical implementation remains available for
compatibility, but new platform behavior should not be added to
`examples/run_followstop.py`.

## Backend Boundary

The canonical external-stack boundary is:

```text
carla_testbed/adapters/base.py
ADStackBackend
```

The core runner should depend on this neutral lifecycle:

1. `prepare(context)`
2. `start()`
3. `publish_inputs(frame_context)`
4. `poll_control(timeout_s)`
5. `collect_diagnostics()`
6. `stop()`

ROS2 native support and `tbio/` remain transitional. Apollo/CyberRT is the
current MVP target, with CI-safe mock adapter tests and a lazy CyberRT skeleton.
See `docs/backends.md` and `docs/apollo_mvp_bridge.md`.

## What Is Still Experimental

- The typed v0 config path exists. Claim-profile Apollo Town01 configs now
  dispatch through `typed_apollo_claim_runtime` instead of silently falling into
  legacy fallback or stopping at "runner not wired." This transition runtime
  still reuses the existing `tbio` / `tools/apollo10_cyber_bridge` execution
  component, but the input is a typed-resolved effective config and the manifest
  records `legacy_fallback_used=false`. Missing online Apollo/CARLA evidence
  remains `insufficient_data`; this is runtime wiring, not a behavior-success
  claim.
- `python -m carla_testbed smoke` is CI-friendly and does not start CARLA or
  Apollo.
- `python -m carla_testbed run --config configs/examples/smoke.yaml --dry-run`
  validates the typed run path.
- Existing heavy Apollo/Town01 work may still use transitional configs, `tbio`,
  `tools/`, and legacy bridge scripts until promoted behind the adapter
  contract.

Do not describe Apollo as fully solved in CARLA unless a specific run artifact
and acceptance document prove that claim.
