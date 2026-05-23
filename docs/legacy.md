# Legacy And Demo Entrypoints

This page explains which repository surfaces are retained for compatibility,
demos, and focused debugging. These paths may still be runnable, but they are
not the intended home for new platform architecture.

## Current Project Boundary

The project is a CARLA-based simulation testbed for autonomous driving
experiments, currently prioritizing a CARLA + Apollo ground-truth MVP closed
loop.

The supported work areas are:

- Core platform: config, runner/harness, simulation utilities, sensors,
  contracts, recording, metrics, and run artifacts.
- Baseline/demo: follow-stop baseline used for smoke tests and demonstrations.
- External backend: ROS2/`tbio` as a transition backend layer; Apollo/CyberRT
  MVP adapter planned or experimental.

## Legacy / Demo Surfaces

| Path | Status | Use It For | Do Not Use It For |
| --- | --- | --- | --- |
| `examples/run_followstop.py` | legacy/demo entrypoint | follow-stop demos, compatibility checks, narrow scenario debugging | new harness architecture, backend contracts, artifact design, Apollo MVP feature growth |
| `examples/legacy/` | legacy documentation | migration notes and reminders for old examples | runnable platform code |
| `io/` | deprecated compatibility layer | older contracts, wrapper scripts, smoke tests | canonical backend ownership or new CyberRT transport design |
| `tbio/` | transition backend layer | existing runnable ROS2/native/backend paths | neutral core contracts or long-term public API design |
| `tools/run_*.py` | operational helpers | regressions, batch runs, analysis, demos | core platform abstractions or new public APIs |
| Autoware configs/adapters | legacy experimental | preserving compatibility and old experiments | current equal-priority platform direction |

## Recommended Runtime Shape

Current canonical no-runtime commands:

```bash
python -m carla_testbed config-validate configs/examples/smoke.yaml
python -m carla_testbed smoke --config configs/examples/smoke.yaml
python -m carla_testbed run --config configs/examples/smoke.yaml --dry-run
```

Planned typed run shape:

```bash
python -m carla_testbed run --config configs/examples/follow_stop.yaml
```

This command shape is the target, but the full typed CARLA runner is not wired
for every scenario yet.

Current compatible follow-stop command:

```bash
python -m carla_testbed run --config configs/io/examples/followstop_dummy.yaml
```

Current Apollo follow-stop compatibility command:

```bash
python -m carla_testbed run --config configs/io/examples/followstop_apollo_gt.yaml
```

These compatibility commands may dispatch through legacy/transitional code.
They are kept so existing demos and debugging paths do not break while the
platform boundary is cleaned up.

## Where New Work Should Go

| New work | Preferred location |
| --- | --- |
| Harness, tick loop, hooks, cleanup | `carla_testbed/runner/`, `carla_testbed/core/` |
| Scenario intent and registry | `carla_testbed/scenarios/` |
| Core state/truth/control models | `carla_testbed/contracts/` |
| CARLA-specific simulation helpers | `carla_testbed/sim/` |
| Sensor rig lifecycle and sync stats | `carla_testbed/sensors/` |
| Controller contracts and CARLA apply boundary | `carla_testbed/control/` |
| Run artifacts and summaries | `carla_testbed/record/` |
| Minimal comparable metrics | `carla_testbed/evaluation/` |
| Backend contract and adapters | `carla_testbed/adapters/` |
| Apollo MVP adapter work | `carla_testbed/adapters/apollo/` |
| Transitional runtime implementation | `tbio/` |
| Durable truths and verified findings | `reference_pack/reference/` |

## Documentation Rule

Do not claim that Apollo is fully complete in CARLA unless there is current run
evidence and acceptance documentation. Describe Apollo/CyberRT work as MVP
planned or experimental unless the relevant config, artifact, and reference
documentation prove a stronger status.

Do not hide legacy/demo paths. They are part of the project history and still
useful for demos, but they should not attract new platform logic.
