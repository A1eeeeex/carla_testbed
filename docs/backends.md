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
| `carla_builtin` backend facade | diagnostic CARLA-only backend | Runs fixed scenes with the built-in ego controller; not Apollo/Autoware evidence. |
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

`carla_testbed.adapters.apollo.cyber_gt_bridge` is the adapter-side facade for
the current bridge entrypoint. New launch/orchestration code should depend on
that facade instead of hard-coding `tools/apollo10_cyber_bridge/bridge.py`.
This does not mean the large bridge runtime has been migrated; it only creates
the seam for moving publishers, subscribers, transforms, and diagnostics into
package modules in small PRs.

## CARLA Builtin Diagnostic Backend

`carla_builtin` is a local CARLA-only diagnostic backend. It controls ego with
`carla_testbed_builtin_controller` and lets `fixed_scene_player` control key
scenario actors. It exists to check scenario geometry and playback quickly
before connecting Apollo or Autoware.

`carla_builtin` can be selected in RunPlan compilation like Apollo or Autoware,
but its claim boundary is always diagnostic-only. A successful builtin run does
not prove Apollo or Autoware natural-driving capability.

For Phase 1 comparison work, `carla_builtin` is the current runtime
implementation of the `PlanningControlBackend` target category. The runtime
name should remain `carla_builtin`; manifest fields carry the category:

- `backend_type=planning_control_backend`
- `input_contract=scene_truth_direct`
- `adapter_path=carla_testbed.scenario_player.builtin_ego_runner`
- `available_truth_fields` listing ego, target actor, fixed-scene, and route
  context truth used by the controller
- `output_control_mode=carla_vehicle_control`

This makes the input asymmetry explicit. Phase 1 compares scenario-level
closed-loop behavior; it is not an identical-input Apollo-vs-builtin algorithm
benchmark.

`apollo_cyberrt` is the Phase 1 `ApolloBackend` / reference backend category
and should declare `backend_type=apollo_reference_backend` plus its
truth-input GT replacement contract in manifests and launch plans. Missing
fixed-scene Apollo runtime artifacts should make a run `invalid`, not an
Apollo behavior loss.

`tools/run_phase1_scenario.py` provides the current CI-safe scaffold for this
boundary. For fixed-scene `apollo_cyberrt` runs it does not start CARLA or
Apollo; it writes `preflight.json` and `analysis/phase1_status/phase1_status.json`
with `backend_not_ready` when runtime migration is absent. This lets catalog and
ScenarioComparison reports show that ApolloBackend is represented but not yet
evaluable for that ScenarioCase. It is not a replacement for an online Apollo
fixed-scene run with obstacle GT, routing/planning/control, and control
attribution artifacts.

For Baguang dynamic fixed scenes under the non-claim `scenario_validation`
gate, the Apollo launch plan may expose a guarded sidecar transition through
the legacy follow-stop runner. That transition disables the legacy front actor
and attaches `fixed_scene_player` as the non-ego target actor owner. It is a
runtime-dispatch seam for online validation, not completed online behavior
evidence. `claim_natural_driving` gates must not use this sidecar as a
claim-grade path unless the run also produces the required fixed-scene runtime,
obstacle GT, v-t-gap, phase1 status, Apollo link, and comparison artifacts.

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
