# Backend And Adapter Boundaries

This repository separates the CARLA testbed core from external autonomous
driving stack runtimes.

## Canonical Boundary

The canonical backend contract starts at:

- `carla_testbed/adapters/base.py`
- `ADStackBackend`

The core runner should depend on this neutral contract and core data models. It
should not directly depend on ROS2, CyberRT, Apollo protobufs, Autoware message
packages, or stack-specific launch details.

The minimal backend lifecycle is:

1. `prepare(context)`
2. `start()`
3. `publish_inputs(frame_context)`
4. `poll_control(timeout_s)`
5. `collect_diagnostics()`
6. `stop()`

## Current Backend Status

| Surface | Status | Role |
| --- | --- | --- |
| `carla_testbed/adapters/base.py` | canonical contract | New backend-facing code should start here. |
| `tbio/` | transition implementation layer | Still used by current runnable ROS2/native and backend launch paths. |
| `carla_testbed.ros2` | transitional ROS2 support | Existing ROS2 GT publisher and utilities; not platform core. |
| `tools/apollo10_cyber_bridge/` | Apollo MVP experimental bridge | Current Apollo/CyberRT work area until promoted behind the adapter contract. |
| `io/` | deprecated compatibility wrapper | Kept for legacy scripts/contracts; do not add new platform logic. |

## ROS2 Native

ROS2 native remains transitional. It may continue to be used for existing
ground-truth and control transport, but new core runner logic should not import
`rclpy` or ROS message packages directly. Keep ROS2 specifics in adapter glue,
`tbio/`, or `carla_testbed.ros2`.

## Apollo / CyberRT

Apollo/CyberRT is the current MVP target backend. For now, bridge experiments
remain under `tools/apollo10_cyber_bridge/`. When promoted, Apollo-specific
code should implement `ADStackBackend` while keeping CyberRT channels and
protobuf conversion out of core runner, scenarios, sensors, recording, and
evaluation.

## Legacy IO

`io/` is deprecated compatibility. It exists so historical scripts and contract
files do not break while the project migrates to clearer config, artifact, and
backend boundaries.

Do not add new platform behavior under `io/`. If a feature is:

- core harness behavior, put it under `carla_testbed/runner`, `contracts`,
  `record`, or `evaluation`;
- transition runtime integration, keep it in `tbio/` for now;
- future stack adapter behavior, start from `carla_testbed/adapters/base.py`;
- Apollo/CyberRT bridge experimentation, keep it in
  `tools/apollo10_cyber_bridge/` until the adapter boundary is ready.

## Current TODO

The existing harness still directly imports some transitional runtime pieces,
notably `tbio.backends.ros2_native`. That is accepted for now. The migration
target is to route these through `ADStackBackend` without changing behavior in a
single large rewrite.
