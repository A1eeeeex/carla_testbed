# Apollo Algorithm Inventory

This document freezes the current Apollo algorithm inventory boundary for the
CARLA GT MVP. Apollo must be treated as a modular stack, not as one opaque
algorithm. A run can prove transport/materialization, routing, planning, or
control evidence without proving that all Apollo modules are reproduced as
upstream Apollo.

Machine-readable inventory:

- `configs/algorithms/apollo_inventory.yaml`

Machine-readable variant manifests:

- `configs/algorithms/apollo_variant.upstream.example.yaml`
- `configs/algorithms/apollo_variant.carla_gt.example.yaml`

## Reproduction Variants

| Variant | Meaning | Upstream-compatible |
| --- | --- | --- |
| `apollo_upstream` | Official Apollo code/config without CARLA testbed tuning. | yes |
| `apollo_replayed` | Apollo behavior inspected from recorded CyberRT/run artifacts. | no |
| `apollo_ported_carla_gt` | Apollo modules driven by CARLA ground-truth bridge inputs. | no |
| `apollo_tuned_town01` | Town01-specific bridge/config/calibration experiments. | no |

Do not report `apollo_tuned_town01` as upstream Apollo. Any change to bridge
semantics, control mapping, calibration, route handling, or Town01-specific
configuration must be named as a tuned or ported variant.

## Variant Manifest Contract

Every Apollo run used as algorithm evidence must reference a stable
`variant_id`. The variant manifest freezes the parts that make an Apollo run
reproducible:

- Apollo version, git commit, Docker image, and Docker image digest.
- Source and config patch paths plus SHA-256 hashes.
- Map name, base map path/hash, and routing map path/hash.
- Vehicle config path/hash and CARLA vehicle blueprint.
- Bridge backend and bridge config path/hash.
- Calibration profile id/path/hash.
- Explicit `allowed_changes` and `forbidden_changes`.

Schema:

- `schema_version: algorithm_variant.v1`
- loader: `carla_testbed.algorithms.variant.load_algorithm_variant`
- validator: `carla_testbed.algorithms.variant.validate_algorithm_variant`

Variant rules:

- `upstream` must not set `source_patch_path` or `config_patch_path`; its bridge
  backend must be `null` or `replay_only`.
- `ported_carla_gt` may adapt map, frame/time, bridge, vehicle binding, and
  calibration references, but `forbidden_changes` must include
  `planning_algorithm_code` and `control_algorithm_code`.
- `tuned_town01` must record `tuning_reason` and a config patch path. It is not
  upstream Apollo and must not be used as an upstream comparison baseline.
- Missing hashes are warnings for example manifests, not silent success for
  final evidence. Before promotion, hashes should be filled from the actual run
  environment and tracked config files.

Run manifests and A/B batches should carry the `variant_id` next to route,
backend, calibration profile, and artifact paths. Without it, a run can only be
used as exploratory evidence, not as algorithm-comparison evidence.

## Current CARLA GT MVP Boundary

The current MVP is a ground-truth closed loop:

- CARLA world/ego/actors provide ground truth localization, chassis feedback,
  obstacles, and traffic-light inputs where available.
- Apollo routing, planning, control, and CyberRT runtime remain the primary
  real Apollo modules under test.
- Native sensor perception is not claimed as reproduced.
- Dreamview is operator/visual evidence, not proof that route health is good.

## Module Inventory

| Module | Responsibility | Main inputs | Main outputs | MVP enabled | Current implementation | Evidence artifacts |
| --- | --- | --- | --- | --- | --- | --- |
| map / hdmap | Provides lane topology and reference-line map source. | Apollo map directory, selected map, bridge map root. | HDMap query availability for routing/planning. | yes | Real Apollo map runtime, with CARLA Town01 map compatibility constraints. | `manifest.json`, `config.resolved.yaml`, `routing_event_debug.jsonl`, `planning_topic_debug_summary.json` |
| routing | Converts route request or lane-follow command into Apollo routing response. | `/apollo/raw_routing_request`, `/apollo/external_command/lane_follow`, `/apollo/localization/pose`, HDMap. | `/apollo/routing_response`. | yes | Real Apollo routing. | `routing_event_debug.jsonl`, `goal_validity_debug.jsonl`, `summary.json`, `route_health.json` |
| localization | Provides ego pose, heading, velocity, and timestamp to Apollo. | CARLA ego transform, velocity, simulation timestamp. | `/apollo/localization/pose`. | yes | Replaced by CARLA GT bridge, not native Apollo localization. | `cyber_bridge_stats.json`, `localization_debug.jsonl`, `timeseries.csv`, `route_health.json` |
| perception | Provides obstacles and traffic-light facts to planning. | CARLA actors, traffic lights, ego pose, bridge filter policy. | `/apollo/perception/obstacles`, `/apollo/perception/traffic_light`. | yes | Replaced by CARLA GT bridge where available; not native sensor perception. | `direct_bridge_actor_snapshot.json`, `cyber_bridge_stats.json`, `obstacle_debug.jsonl`, `summary.json` |
| prediction | Predicts future behavior of perceived objects. | `/apollo/perception/obstacles`, `/apollo/localization/pose`, `/apollo/routing_response`. | `/apollo/prediction`. | no | Not a current MVP capability signal; GT-driven obstacle inputs may bypass native prediction evidence. | `cyber_channel_status.json`, Apollo module logs, `summary.json` |
| planning | Generates trajectory from route, map, localization, chassis, and obstacles. | `/apollo/routing_response`, `/apollo/localization/pose`, `/apollo/canbus/chassis`, `/apollo/perception/obstacles`, optional `/apollo/prediction`. | `/apollo/planning`. | yes | Real Apollo planning. | `planning_topic_debug.jsonl`, `planning_topic_debug_summary.json`, `planning_trajectory_type_summary.finalized.json`, `route_health.json` |
| control | Converts planning trajectory and vehicle state into throttle/brake/steer command. | `/apollo/planning`, `/apollo/localization/pose`, `/apollo/canbus/chassis`. | `/apollo/control`. | yes | Real Apollo control, followed by bridge mapping to CARLA actuation. | `control_handoff_summary.json`, `cyber_bridge_stats.json`, `timeseries.csv`, `direct_bridge_control_apply.jsonl` |
| canbus / chassis | Reports vehicle speed and control feedback to Apollo. | CARLA ego speed, applied control, simulation timestamp. | `/apollo/canbus/chassis`. | yes | Replaced by CARLA GT chassis bridge. | `cyber_bridge_stats.json`, `timeseries.csv`, `direct_bridge_control_apply.jsonl`, `summary.json` |
| guardian / monitor | Tracks Apollo module health and safety status. | Apollo module status, planning/control health. | Monitor/guardian status and logs. | no | Not a current promotion signal, but useful for Apollo-side diagnosis. | Apollo module logs, monitor logs, `summary.json` |
| dreamview | Provides UI/HMI and visual inspection of Apollo channels. | Apollo runtime status, map, vehicle, CyberRT channels. | Dreamview HMI state and visualization. | yes | Real operator UI. It does not prove planning/control health by itself. | `dreamview_launch.log`, screenshot/video, `town01_demo_recording_inspection.json`, `manifest.json` |
| cyberrt / cyber_recorder | Transports Apollo channels and records runtime evidence. | Apollo runtime environment, channel readers/writers, bridge time policy. | Channel transport, logs, cyber records. | yes | Real CyberRT runtime. | `cyber_bridge_stats.json`, `bridge_transport_summary.json`, Apollo planning/control logs, cyber recorder files |

## Evidence Rules

- `routing_success_count > 0` proves routing materialization only; it does not
  prove planning or route health.
- `/apollo/planning` with non-empty trajectory proves planning materialization,
  but curve health still needs route-health and lateral/heading evidence.
- `/apollo/control` proves control output availability, but not correct CARLA
  actuation mapping.
- CARLA movement proves actuation happened, but not that Apollo lateral
  semantics are healthy.
- Dreamview readiness is useful operator evidence, not an acceptance result.

## Use In Future Algorithm Comparisons

When comparing Apollo with another open-source AD stack, record:

- algorithm inventory version
- variant name
- enabled modules
- GT/mock/bridge replacements
- route set and artifact completeness
- no-regression gate results
- calibration profile, if any

This prevents a tuned CARLA GT bridge run from being mistaken for upstream
Apollo, and prevents platform transport failures from being reported as
algorithm capability failures.
