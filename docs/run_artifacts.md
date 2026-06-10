# Run Artifacts

This document defines the minimum self-contained output surface for a CARLA
testbed run. A run directory should answer four questions: what was requested,
what config was resolved, what happened, and what evidence can be compared.

## Output Directory Structure

The standard layout is:

```text
runs/<run_id>/
  manifest.json
  config.resolved.yaml
  summary.json
  events.jsonl
  timeseries.csv
  logs/
```

`timeseries.jsonl` is also supported by `RunArtifactStore` for future backends.
The legacy harness still writes CSV-oriented frame data.

## `manifest.json`

`manifest.json` records static run context and identity. Minimum fields:

- `run_id`
- `start_time_wall_s`
- `end_time_wall_s`, usually added at close
- `config_path`
- `git_sha`, when available
- CARLA `host`, `port`, and `town`, when known
- `carla_world`, when runtime CARLA was available. This records
  `configured_town`, the observed `loaded_map_name`, short-name match status,
  and spawn-point count so evidence can distinguish requested map identity from
  the world actually loaded by CARLA.
- `scenario_name`
- `backend_name`
- non-critical metadata such as smoke mode or runtime notes

Use the manifest to identify what the run claimed to execute. Do not use it as
a replacement for per-frame evidence.

## `config.resolved.yaml`

`config.resolved.yaml` is the resolved configuration snapshot for the run. It
should reflect built-in defaults, main config, local override, and CLI
overrides after merging.

Do not commit resolved configs that contain private host paths or secrets.
Tracked examples should use environment placeholders or local override
templates instead.

## `summary.json`

`summary.json` is the final result surface. Minimum fields:

- `success`
- `exit_reason`
- `frames`
- `sim_duration_s`
- `wall_duration_s`
- `cleanup_errors_count`
- `metrics`

The `metrics` block is intentionally small and comparable:

- `frames`
- `sim_duration_s`
- `wall_duration_s`
- `collision_count`
- `lane_invasion_count`, when available
- `avg_speed_mps`
- `max_speed_mps`
- `min_lead_distance_m`, when available
- `control_frames`
- `exit_reason`

If a value cannot be computed, use `null` or an explicit unknown value. Do not
invent metrics to make a run look healthier.

## `events.jsonl`

`events.jsonl` stores discrete run events, one JSON object per line. Typical
events include:

- `run_start`
- `smoke_start`
- `collision`
- `lane_invasion`
- `backend_warning`
- `failure`
- `run_end`

Events should be useful for debugging without scanning a full timeseries.

## `timeseries.csv` / `timeseries.jsonl`

Timeseries files store frame-level evidence. The legacy harness CSV path may
include:

- frame id and simulator time
- ego speed
- lead distance when available
- raw control command
- clamped/applied control
- collision and lane invasion counters
- controller debug fields

Future backends may prefer `timeseries.jsonl` for structured per-frame records.
Both forms should stay run-local.

## `logs/`

`logs/` is optional. It may contain captured simulator, backend, bridge, or
adapter logs. For no-runtime smoke runs it may be empty.

## Town01 Truth-Input Diagnostics

Town01 Apollo truth-input runs that are used for natural-driving or curve
diagnosis need richer evidence than the base artifact surface. Expected
run-local diagnostics include:

- `analysis/route_health/route_health.json`
- `analysis/route_health/route_health.csv`
- `analysis/route_health/curve_segments.csv`
- `analysis/route_health/route_health_summary.md`
- `artifacts/carla_tick_health.jsonl`
- `artifacts/carla_tick_health_summary.json`
- `artifacts/topic_publish_stats.jsonl`
- `analysis/apollo_channel_health/apollo_channel_health_report.json`
- `analysis/localization_contract/localization_contract_report.json`
- `analysis/localization_contract/localization_contract_summary.md`
- `analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json` when
  `artifacts/apollo_hdmap_projection.jsonl` is exported from Apollo HDMap API
- `analysis/apollo_hdmap_projection/apollo_hdmap_projection_summary.md` when
  the projection report is generated
- `analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json`
- `analysis/apollo_module_consumption/apollo_module_consumption_report.json`
- `analysis/apollo_control_handoff/apollo_control_handoff_report.json`
- `artifacts/control_apply_trace.jsonl`
- `analysis/control_health/control_health_report.json`
- `analysis/apollo_link_health/apollo_link_health_report.json`
- `analysis/apollo_link_health/apollo_link_health_summary.md`
- `analysis/failure_timeline/failure_timeline_report.json`
- `analysis/route_start_alignment/route_start_alignment_report.json`
- `analysis/artifact_completeness/artifact_completeness_report.json`
- `analysis/obstacle_gt_contract/obstacle_gt_contract_report.json` for obstacle or follow-stop scenarios
- `artifacts/fixed_scene_resolved.json` for scripted non-ego scenario actors
- `artifacts/fixed_scene_runtime_state.json` for runtime spawn/control adapter state
- `artifacts/scenario_actor_trace.jsonl` when fixed scene playback is enabled
- `artifacts/scenario_phase_events.jsonl` when fixed scene playback is enabled
- `analysis/fixed_scene_contract/fixed_scene_contract_report.json` when fixed scene playback is enabled
- `analysis/scenario_actor_contract/scenario_actor_contract_report.json` when fixed scene playback is enabled
- `analysis/traffic_light_contract/traffic_light_contract_report.json` for traffic-light scenarios
- `analysis/traffic_light_behavior/traffic_light_behavior_report.json` or legacy
  `analysis/traffic_light/traffic_light_behavior_report.json` for traffic-light scenarios
- `analysis/natural_driving/natural_driving_report.json`
- `analysis/natural_driving/natural_driving_report.csv`
- `analysis/natural_driving/natural_driving_summary.md`

`natural_driving_report.json` must treat a missing
`artifact_completeness_report.json` as `insufficient_data`. This keeps the
evidence chain persisted in the run directory instead of relying on an in-memory
check or visual observation.

Fixed-scene artifacts describe scripted non-ego actor setup and behavior.
They are useful for follow-stop, cut-in, cut-out, and lead-vehicle diagnostic
cases, but they do not prove ego natural-driving success. Ego capability claims
still require the Apollo/Autoware control, localization, reference-line,
perception, no-assist, and natural-driving gates.

For fixed-scene playback, phase start is not enough. Required phases default to
`required: true`, and `fixed_scene_contract_report.json` must show that they
started and completed. Cut-in/cut-out traces must also contain observed
longitudinal/lateral evidence; `lane_change_progress` alone is only scripted
intent and cannot validate a completed lane change. Scripted CARLA
`set_transform` lane changes are diagnostic-only unless a future physics
controller and no-teleport evidence upgrades the contract.

`artifact_completeness_report.json` also treats missing or mismatched
`manifest.json.carla_world` identity as `insufficient_data`. A run may request
`Town01` in config, but capability evidence should separately record the CARLA
map that was actually loaded.

`carla_tick_health_summary.json` records the harness world-tick owner,
successful tick count, tick failures, and maximum wall-time tick duration. If
it reports `last_failure_reason=CARLA_WORLD_TICK_TIMEOUT` before routing
materializes, the run is an environment/world blocker. Do not rewrite that as
Apollo routing failure or natural-driving behavior evidence.

The same artifact also distinguishes `max_tick_wall_duration_s`, the duration
of a single `world.tick()` call, from `max_inter_tick_wall_interval_s`, the
wall-time interval between harness ticks. A run can have no CARLA
`world.tick()` timeout while still showing a long inter-tick wall pause. When a
Planning channel gap appears on Apollo header/wall time but not on sim time,
check the inter-tick interval before blaming CARLA map semantics or control
mapping. New runs also write `frame_loop_timing` rows in
`artifacts/carla_tick_health.jsonl`; these rows expose the slowest harness stage
between ticks, such as GT publish hooks, artifact recording, or demo capture.
Legacy tick callbacks are expanded into `hook.<method>.<callback_source>`
entries so a long aggregate hook stage can be attributed to the specific
scenario or Apollo backend callback. For Apollo CyberRT runs, deferred Control
startup is scheduled asynchronously by default; startup evidence remains in
`artifacts/apollo_backend_startup_trace.jsonl` and
`artifacts/apollo_control_deferred_*.log`, while tick-health should no longer
show a multi-second `CyberRTBackend.on_sim_tick` wall pause. This cadence
evidence is not behavior success evidence.

For no-interference Apollo truth-input claims, `apollo_link_health_report.json`
is the first triage index, while `natural_driving_report.json` is the final
suite gate. Missing localization, reference-line, control-handoff,
traffic-light, obstacle, or assist-ledger evidence must remain
`insufficient_data`; recording videos or Dreamview screenshots do not promote a
run to claim-grade evidence.

`apollo_link_health_report.json` aggregates environment/world, bridge runtime,
channel health, GT localization, HDMap projection, planning reference-line,
route establishment, Apollo module-consumption, routing/planning/control
handoff, control mapping/apply, obstacle GT, traffic-light GT, no-assist
boundary, and natural-driving outcome
layers. A missing applicable layer blocks
`can_claim_unassisted_natural_driving`; traffic-light evidence may be
`not_applicable` only for non-traffic-light scenarios.
`apollo_reference_line_contract_report.json` should be read as three layers:
planning trajectory, control reference, and official Apollo HDMap projection.
The first two are diagnostic/reference-line semantics evidence; the HDMap
projection layer is required before a lane/reference-line hard gate can become
claim-grade.

`apollo_hdmap_projection_report.json` is a direct inspection report for
`artifacts/apollo_hdmap_projection.jsonl`. It is useful for checking whether
the artifact is official Apollo HDMap API evidence before the broader
reference-line and localization contracts consume it. A missing projection
report or missing JSONL should remain `insufficient_data`; do not substitute
bridge nearest-lane diagnostics or CARLA waypoint projection.
Claim-grade projection evidence requires official `source=apollo_hdmap_api`,
`ok_ratio >= 0.95`, at least the configured minimum sample count, heading
`p95 < 0.05 rad`, lateral error `p95 < 0.50 m`, and lane-id compatibility with
Planning `lane_id` or `target_lane_id` evidence. The report also records
sim-time, projection-s, route-s, and map-identity coverage. Claim-grade
projection must cover enough of the evaluated run and must not mix map names or
map directories; a handful of valid points near startup is diagnostic evidence,
not route-level HDMap projection proof.

When `apollo_reference_line_contract_report.json` is regenerated from fallback
planning/control/timeseries artifacts, the analyzer uses timestamp/as-of joins
with a default 50 ms tolerance. Rows outside that tolerance are dropped from
claim-grade metrics and reported through `fallback_join_coverage_ratio` and
`fallback_join_dropped_unaligned_rows`. Index-based merging is not acceptable
claim evidence.

`artifacts/topic_publish_stats.jsonl` is row-level bridge publish evidence for
claim-grade Apollo channel health. It records each published channel with wall
time, sim/header timestamp, sequence number, frame id, CARLA world frame, and
payload counts such as obstacle or traffic-light count. Channel health should
prefer this artifact over aggregate bridge counters when it is present, because
aggregate counters cannot distinguish repeated cached publish attempts from
fresh CARLA world-frame samples.

`artifacts/publish_gap_trace.jsonl` is bridge-side attribution evidence for
missed expected GT publishes or publish-loop overruns. It records world frame,
sim time, localization/chassis publish flags, skip reason, publish-loop
duration, snapshot age, CARLA tick gap, payload-build duration, stats-write
duration, writer write duration, and async artifact queue depth. It is
diagnostic evidence only: a gap with a clear reason is still a channel-health
finding until the run satisfies the configured max-gap threshold.

For Apollo planning, `channel_stats.json` may record both the primary Apollo
header/wall-time axis and a secondary `sim_time_*` axis when
`planning_topic_debug.jsonl` contains `sim_time_sec`. A small sim-time gap does
not erase a large Apollo header/wall-time gap: if Control consumes expired
trajectories or Planning pauses in wall time, channel health must remain a
blocking failure and should report the time-axis diagnosis explicitly.

`analysis/planning_materialization/planning_materialization_report.json`
explains whether Apollo Planning actually materialized non-empty
`ADCTrajectory` output after routing and required channel evidence. It reports
the non-empty trajectory ratio, longest empty streak, route-establishment
status, and an empty-reason histogram from planning route-segment/reference-line
debug artifacts when present. It also includes `empty_asof_join`, which aligns
empty Planning rows to nearest localization, chassis, reference-line, and HDMap
projection evidence within the configured tolerance; missing row-level joins
keep the interpretation diagnostic. Control rx/tx evidence must not override a
`planning_trajectory_materialization_low` or `route_establishment_latency`
blocker.

`analysis/apollo_module_consumption/apollo_module_consumption_report.json`
checks whether Apollo modules appear to consume the GT inputs that the bridge
publishes. It inspects planning materialization, planning debug summaries,
planning/control/routing debug rows, topic publish rows, prediction evidence,
and Apollo logs for input timeouts, reference-line provider failures,
prediction-not-ready messages, and empty-reason attribution. This is stricter
than bridge-side publish evidence: a channel can be published while Planning
still does not consume it.

`analysis/chassis_gt_contract/chassis_gt_contract_report.json` checks the GT
replacement contract for `/apollo/canbus/chassis`: channel count/rate/gaps,
timestamp/sequence monotonicity, chassis speed versus ego/localization speed,
driving mode, gear, error code, and available throttle/brake/steer feedback.
Missing chassis contract evidence is `insufficient_data`; it cannot be replaced
by a generic channel-health pass when making no-interference Apollo claims.

`analysis/traffic_flow_contract/traffic_flow_contract_report.json` checks
optional CARLA Traffic Manager background traffic. It verifies seed recording,
requested/spawned vehicle count, TM/world synchronous mode consistency, unique
`background_vehicle_*` role names, and that ego or scripted scenario actors were
not registered to TM. Passing this report only validates background traffic
setup; it is not an Apollo/Autoware behavior pass.

`analysis/pedestrian_flow_contract/pedestrian_flow_contract_report.json` checks
optional CARLA WalkerAIController background pedestrians. It verifies seed
recording, requested/spawned walker count, controller startup, movement trace
rows in `artifacts/walker_flow_trace.jsonl`, unique `background_walker_*` role
names, and that ego or scripted scenario actors were not registered as
background walkers. Missing movement trace is `insufficient_data`, because a
started WalkerAIController does not prove that the walker actually moved.
Passing this report only validates background pedestrian setup; pedestrian
perception/avoidance claims still need `obstacle_gt_contract_report.json` with
a pedestrian section. For Apollo/Autoware runs with background vehicles or
walkers, `obstacle_gt_contract_report.json` must link each spawned background
actor id from `artifacts/traffic_flow_manifest.json` to a stable Apollo
perception id and the expected object type. Background vehicles or walkers
missing from GT obstacle evidence block claim-grade behavior interpretation.
Nonzero `walker_cross_factor` is allowed for smoke/demo traffic, but it is
reported as a claim blocker unless the run explicitly allows random pedestrian
road crossing.

## Platform Evidence Bundle And Claim Package

RunPlan-driven postprocess can write:

- `analysis/evidence_bundle/evidence_bundle.json`
- `analysis/gate/gate_report.json`
- `analysis/gate/gate_summary.md`
- `launch_plan.json` for `run --plan --dry-run`
- `platform_execution_result.json` for platform executor previews

`evidence_bundle.json` is an index over existing reports and raw artifacts. It
does not run analyzers by itself and does not convert missing reports into
success. `gate_report.json` evaluates both report status and structured rules.
For claim-grade gates, metric rules can fail even when a report advertises
`status=pass`; examples include low Planning non-empty trajectory ratio, missing
route establishment, missing HDMap projection claim grade, non-Apollo applied
control source, or blocking assists.

Claim evidence packages include fixed-scene row-level artifacts when a run
declares fixed-scene playback. If `fixed_scene_resolved.json` is present but
`fixed_scene_runtime_state.json`, `scenario_actor_trace.jsonl`, or
`scenario_phase_events.jsonl` is missing, the package manifest reports
`claim_reproducibility_level=summary_only_missing_fixed_scene_row_level` and
`status=insufficient_data`.

`python -m carla_testbed pack --profile claim` packages review evidence without
large media by default. Claim packages should include row-level evidence when
available:

- `artifacts/topic_publish_stats.jsonl`
- `artifacts/publish_gap_trace.jsonl`
- `artifacts/control_apply_trace.jsonl`
- `artifacts/planning_topic_debug.jsonl`
- `artifacts/control_decode_debug.jsonl`
- `artifacts/apollo_reference_line_contract.jsonl`
- `artifacts/apollo_hdmap_projection.jsonl`
- `artifacts/obstacle_gt_contract.jsonl`
- `artifacts/traffic_light_contract.jsonl`
- `artifacts/traffic_flow_events.jsonl`
- `artifacts/walker_spawn_candidates.jsonl`

The archive contains `package_manifest.json`, which records included files,
omitted large artifacts, missing required row-level evidence, and
`claim_reproducibility_level`. Missing row-level evidence keeps a claim package
at summary-only review level; it must not be described as claim-grade natural
driving evidence.

Operator workflow should generate this report before interpreting control
behavior:

```bash
python tools/analyze_apollo_planning_materialization.py \
  --run-dir runs/<run_id> \
  --out runs/<run_id>/analysis/planning_materialization
```

`analysis/prediction_evidence/prediction_evidence_report.json` records whether
`/apollo/prediction` was natively observed, explicitly bypassed with GT
obstacles for a permitted case, missing, or not required. `/apollo/perception`
obstacles do not count as prediction evidence. Link-health treats missing or
unknown prediction state as `insufficient_data` for claim boundaries.

`artifacts/control_apply_trace.jsonl` is the preferred row-level control-chain
evidence for Apollo CyberRT truth-input runs. Each row preserves Apollo raw
control, bridge mapped control, CARLA applied control, vehicle response,
latency, route progress, actuator mapping mode, calibration profile, steering
sign/scale, and apply-cadence diagnostics. It exists to make
raw -> mapped -> applied -> response attribution auditable; it must not be used
to hide raw Apollo command oscillation or to bypass localization/reference-line
failures.

`artifacts/control_decode_debug.jsonl` and
`artifacts/bridge_control_decode.jsonl` remain fallback row-level Apollo control
decode evidence. They may supply Apollo raw command and bridge mapped command
layers when P0 `timeseries.csv` does not contain external-stack raw/mapped
fields or when `control_apply_trace.jsonl` is unavailable. They do not replace
CARLA applied-control evidence; mapped-to-applied and vehicle response checks
still need `control_apply_trace.jsonl`, applied-control timeseries, direct apply
rows, or vehicle response artifacts.

Postprocess may regenerate `analysis/localization_contract/` only when strong
localization fields or bridge localization stats are present. Plain ego P0
timeseries can support route-health diagnostics, but it must not overwrite an
existing localization contract with weaker reconstructed evidence.

## Current Implementation

`carla_testbed.record.artifact_store.RunArtifactStore` owns the standard file
paths and writers:

- manifest writes and updates
- resolved config snapshot
- summary writes
- event writer
- timeseries writer

The current runner is partially migrated. It writes the standard artifact
surface for new smoke/config paths, while some legacy and Town01 operational
tools still have richer historical artifact families. Preserve those until the
new artifact surface can represent the same evidence.

## Inspecting Runs

Use:

```bash
python -m carla_testbed inspect-run <run_dir>
```

This reads `manifest.json` and `summary.json` and prints a compact result. It
does not replace detailed artifact review for Apollo/Town01 capability
promotion.
