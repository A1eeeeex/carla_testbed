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
- `algorithm_variant_id` and `algorithm_variant_manifest_path` for any
  capability claim. The variant manifest must resolve inside the package and
  identify the same variant; missing or mismatched variant metadata keeps the
  run non-claim-grade.
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
- `analysis/channel_cadence_diagnosis/channel_cadence_diagnosis_report.json`
- `analysis/channel_cadence_diagnosis/channel_cadence_diagnosis_summary.md`
- `analysis/localization_contract/localization_contract_report.json`
- `analysis/localization_contract/localization_contract_summary.md`
- `analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json` when
  `artifacts/apollo_hdmap_projection.jsonl` is exported from Apollo HDMap API
- `analysis/apollo_hdmap_projection/apollo_hdmap_projection_summary.md` when
  the projection report is generated
- `artifacts/routing_response_decoded.json` and
  `artifacts/routing_response_decoded.jsonl` when a real Apollo
  `RoutingResponse` is observed. Apollo 10's routing module can emit on
  `/apollo/raw_routing_response`; the bridge may relay that same protobuf to
  the planning-facing `/apollo/routing_response` channel, but it must not
  synthesize a response from the scenario route.
- `artifacts/routing_event_debug.jsonl` should show
  `routing_phase=claim` and `routing_request_kind=claim_route` for claim or
  materialization probes that disable startup and long-goal routing. A
  `long_phase_route` event is useful diagnostic evidence but is not a clean
  scenario-route claim.
- `artifacts/route_definition_claim.json` records the claim-route ledger built
  from scenario route samples, Apollo lane-window signature, start/goal
  lane/projection fields, route length source, and lane-equivalence status. It
  is generated by the route-contract analyzer when postprocess writes inside a
  run directory. This artifact is route identity evidence only; it does not
  prove ego behavior or Planning/Control quality.
- `route.json` should contain the configured scenario route geometry. For typed
  Apollo transition runs, the runtime materializes it from
  `artifacts/scenario_metadata.json:route_trace` when that trace exists. If the
  runtime can only write `route_stub.v1` with `points=[]`, the run has route
  traceability but no physical route geometry evidence and cannot satisfy a
  claim-grade route gate.
- `analysis/routing_response_decoded/routing_response_decoded_report.json` when
  the decoded routing response is normalized during postprocess
- `analysis/runtime_claim_boundary/runtime_claim_boundary_report.json` when
  platform evidence/gate analysis is run. This materializes the typed-config
  and no-legacy-fallback boundary instead of relying only on the virtual
  evidence-bundle summary.
- Claim-profile typed configs enter an explicit Apollo claim-runtime dispatch
  instead of a legacy fallback. Town01 online drivers use
  `typed_apollo_claim_runtime`, which writes a typed-resolved legacy effective
  config for the transition backend and then invokes the existing CARLA /
  Apollo bridge runtime. Minimal CI configs without a runnable driver use the
  artifact-only `compat_apollo_cyber_gt_runtime`. Either path must keep missing
  routing, HDMap projection, localization, planning, or control evidence as
  `insufficient_data`; dispatch alone cannot support a natural-driving claim.
- The transition runtime must use a CARLA-compatible Python interpreter for the
  online child process. It records the selected interpreter in
  `artifacts/typed_transition_runtime.json`. If the requested run directory is
  already non-empty before launch, the runtime fails preflight instead of
  allowing the legacy runner to redirect evidence into a sibling `__NN`
  directory.
- `analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json`
- `analysis/apollo_module_consumption/apollo_module_consumption_report.json`
- `analysis/apollo_control_handoff/apollo_control_handoff_report.json`
- `artifacts/control_apply_trace.jsonl`
- `analysis/control_health/control_health_report.json`
- `analysis/apollo_link_health/apollo_link_health_report.json`
- `analysis/apollo_link_health/apollo_link_health_summary.md`
- `analysis/gt_replacement_evidence/gt_replacement_evidence_report.json`
- `analysis/gt_replacement_evidence/gt_replacement_evidence_summary.md`
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

`channel_cadence_diagnosis_report.json` is an explanation artifact for
`apollo_channel_health_report.json`. It separates header/sim-time cadence,
wall-time delivery cadence, stale or duplicate timestamp evidence, and
publisher gap traces. When `artifacts/topic_publish_stats.jsonl`,
`artifacts/publish_gap_trace.jsonl`, and `artifacts/carla_tick_health.jsonl`
exist, it also reports top gap windows with skip-reason and CARLA tick-stage
correlation. It must not turn a channel-health failure into a pass; instead it
should explain whether the next fix belongs in GT publisher cadence, Apollo
output cadence, artifact delivery/time-axis interpretation, or missing evidence.
The top-level `primary_gap_source`, `top_gap_window`, `carla_tick_stage`,
`publish_skip_reason`, and `artifact_writer_lag_ms` fields are the operator
attribution surface. If the publish gap trace is absent, the report must say
`missing_publish_gap_trace` instead of guessing a source.

`artifact_completeness_report.json` separates declared summary/report
completeness from physical raw evidence completeness. Claim or materialization
profiles require
timestamped row-level artifacts and route identity inputs such as
`config.resolved.yaml`, `events.jsonl`, `timeseries.csv` or
`timeseries.jsonl`, `route.json`, `route_definition_claim.json`,
`topic_publish_stats.jsonl`, `routing_event_debug.jsonl`,
`routing_response_decoded.json`,
`routing_response_decoded.jsonl`, `planning_topic_debug.jsonl`,
`planning_route_segment_debug.jsonl`, `control_apply_trace.jsonl`,
`control_decode_debug.jsonl`, and `apollo_hdmap_projection.jsonl`. If those raw
artifacts are missing, the report must set `raw_evidence_complete=false` and
`status=insufficient_data` even when high-level summaries are present. Present
but empty raw files are also not physical evidence: for example an empty
`apollo_hdmap_projection.jsonl` must be reported as incomplete instead of
passing because the path exists. Row-level routing evidence must also contain
decoded lane segments, and `route.json` must contain concrete route points. A
stale `routing_response_decoded.jsonl` row such as `message_count=0` or
`status=insufficient_data` is not complete raw evidence, even if a later summary
file exists.

For transition runs, claim/runtime manifests should use
`transport_mode=apollo_cyberrt_gt_over_ros2_transition` as the canonical
identity. Legacy compatibility with the old ROS2 GT path is recorded separately
as `legacy_transport_name=ros2_gt` and `compat_layers`. Some historical tools
and fixtures still use `ros2_gt` as an input option or baseline label; that
legacy name must not be read as a carla_direct promotion or as a different
claim-grade runtime.

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
`channel_stats.json.source.publish_gap_trace_summary` summarizes this file for
offline review by separating stale-sample skip, publish-loop overrun, artifact
backpressure, writer duration, and async queue pressure. The summary explains
why a gap happened; it does not turn skipped samples into fresh claim-grade
localization or chassis evidence.

For Apollo planning, `channel_stats.json` may record both the primary Apollo
header/wall-time axis and a secondary `sim_time_*` axis when
`planning_topic_debug.jsonl` contains `sim_time_sec`. A small sim-time gap does
not erase a large Apollo header/wall-time gap: if Control consumes expired
trajectories or Planning pauses in wall time, channel health must remain a
blocking failure and should report the time-axis diagnosis explicitly.
Planning gaps over roughly 500 ms should remain visible as warnings even when a
scenario-specific hard fail threshold is higher, because they can explain
stuttered control consumption or stale planning materialization.

`analysis/apollo_route_contract/apollo_route_contract_report.json` checks that
Apollo Routing materialized the intended scenario route, in Apollo map frame.
It records raw CARLA scenario coordinates separately from transformed Apollo-map
coordinates, splits startup routing from claim-route materialization, and
blocks hard claims when only an ego-seed startup route exists.
The preferred route-response source is `artifacts/routing_response_decoded.json`
or `artifacts/routing_response_decoded.jsonl`, decoded from an observed Apollo
`RoutingResponse`. For Apollo 10 raw-routing paths, the decoded source may be
`/apollo/raw_routing_response`, with a recorded relay to the planning-facing
`/apollo/routing_response`. If that artifact is missing, older
Planning-derived route summaries remain diagnostic fallback evidence, but they
must not produce claim-grade `pass` by themselves.
It also records the route identity chain:
`configured_scenario_route`, `last_routing_request`, `last_routing_response`,
and `latest_planning_active_route_segment`. If the scenario route, bridge
request, Apollo routing response, or Planning active segment disagree in route
length, lane signature, phase, or goal snap compatibility, the report must
surface `route_identity_inconsistent` rather than allowing later control
activity to imply the intended route was consumed. A near-threshold goal XY
match is only a warning unless lane/Frenet snap evidence is compatible.
The configured scenario route is also checked internally. Town01 route-health
metadata may retain a legacy `route_length_m` used for corpus selection or
straight-line goal-window filtering; that field is not claim-grade route
identity by itself. Claim profiles should write `claim_route_length_m` and
`route_trace_length_m`, preferably from the route trace s-span or polyline.
If a run writes a `route_stub.v1` with an empty `points` list, treat it as
source traceability only. Postprocess should not crash on that stub and may
fall back to manifest `route_trace` or timeseries route fields, but any such
fallback must keep the route evidence level explicit.
Lane identity has an additional namespace boundary. Town01 scenario
`route_trace` lanes are usually CARLA waypoint road/section/lane ids, while
Apollo Routing lanes are Apollo HDMap lane ids. If the normalized lane keys
match exactly, `apollo_route_contract_report.json` may record
`lane_equivalence_status=direct_match`. If they do not match but route length
and start/goal are compatible across the configured frame transform, the report
must not hard-fail the route just by comparing CARLA waypoint ids with Apollo
HDMap ids. It should record
`lane_equivalence_status=cross_namespace_unverified`, require
`apollo_hdmap_projection_for_lane_equivalence`, and keep the run
`insufficient_data` until official Apollo HDMap projection or an explicit lane
equivalence artifact is present.
If the Apollo routing request itself includes trusted start/goal projections
onto Apollo lanes, the route contract may use those projections to avoid a
misleading lane-sequence hard fail across namespaces. That still does not prove
lane equivalence; the report should keep
`lane_equivalence_status=cross_namespace_unverified` until official HDMap
projection or an explicit lane-equivalence artifact closes the gap.
`apollo_route_contract_report.json` uses the claim/trace length for
`scenario_route_length_m` when present and still preserves the legacy field as
`scenario_route_legacy_length_m` / `scenario_route_legacy_length_role` for
auditability. If an older artifact has only a legacy declared length and it
disagrees with the `route_trace` s-span or polyline length, the report records
`scenario_route_length_consistency_status=inconsistent` and blocks the claim
with `scenario_route_length_inconsistent`. This prevents a run from comparing
Apollo Routing against a stale or partial route length while the richer trace
describes a different path.
For claim or materialization profiles, `artifacts/scenario_goal.json` must use
Apollo map coordinates in the top-level `goal` with `frame="apollo_map"`.
Raw CARLA coordinates may be preserved as `goal_raw_carla` for audit, but they
must not be consumed as the claim-routing goal. This is especially important for
Town01 truth-input runs where ROS/CARLA ground-truth publishers may already
apply the CARLA-to-Apollo axis mapping before the Cyber bridge receives odom.

`apollo_hdmap_projection_report.json` separates projection quality from
projection coverage. High Apollo HDMap heading/lateral error, low ok ratio, or
inconsistent map identity is a failure. Too few samples or too little
projection-s coverage is `insufficient_data`: it still blocks a claim, but the
next validation is a longer or denser claim-window projection export rather
than treating Apollo Routing or map alignment as proven wrong.

`analysis/route_identity/route_identity_report.json` and
`analysis/routing_contract/routing_contract_report.json` are narrower
postprocess views derived from the same evidence. `route_identity_report.json`
is the oracle-style artifact for scenario route, Apollo routing lane sequence,
Planning reference-line lane sequence, start/goal pose, and route length
agreement. `routing_contract_report.json` focuses on routing request/response
compatibility and whether the claim route materialized. These reports are
allowed to fail before Planning or Control are interpreted; a failed route
identity contract means later movement/control evidence is not evidence for the
configured route.
Both derived reports preserve the scenario route length source, claim length,
legacy length, trace length, and consistency fields from
`apollo_route_contract_report.json`, so a stale metadata length or conflicting
route trace remains visible even when operators open only the narrower
route-identity or routing-contract summaries.

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
Natural-driving reports expose both overall Planning non-empty ratio and the
claim-window ratio, including `planning_nonempty_ratio_source`,
`planning_nonempty_ratio_overall`, `planning_nonempty_ratio_filtered`, and
`route_establishment_source`. A filtered claim-window ratio can prevent startup
empty messages from diluting the metric, but it does not override a failed
`planning_materialization_report.json` or `route_established=false`.

`analysis/apollo_module_consumption/apollo_module_consumption_report.json`
checks whether Apollo modules appear to consume the GT inputs that the bridge
publishes. It inspects planning materialization, planning debug summaries,
planning/control/routing debug rows, topic publish rows, prediction evidence,
and Apollo logs for input timeouts, reference-line provider failures,
prediction-not-ready messages, and empty-reason attribution. This is stricter
than bridge-side publish evidence: a channel can be published while Planning
still does not consume it.
The report also echoes consumed route identity fields from route-contract
evidence. `routing_response_consumed_by_planning=true` only proves Planning
consumed a routing response; it does not prove that the consumed response is the
configured claim route.
For online startup windows, a small number of early
`reference_line_provider_not_ready` or route/reference-line log patterns is
diagnostic only if Planning later materializes the claim route with a high
after-routing non-empty ratio. If route establishment is absent, delayed, or
low-ratio, the same patterns remain blocking evidence.
For compatibility runtimes that pre-create placeholder reports, link-health may
recompute planning materialization and module-consumption layers from existing
row artifacts such as `planning_topic_debug.jsonl`,
`planning_route_segment_debug.jsonl`, `routing_event_debug.jsonl`,
`topic_publish_stats.jsonl`, and `control_decode_debug.jsonl`. This fallback is
only valid for explicit placeholder reports such as
`planning_runtime_messages_missing` or `apollo_module_runtime_logs_missing`; a
missing report remains missing, and a real semantic `fail` is not overwritten.
When natural-driving postprocess writes
`analysis/natural_driving_postprocess/natural_driving_report.json`,
link-health treats it as the same acceptance artifact class as
`analysis/natural_driving/natural_driving_report.json` and uses the newest
available report. This prevents an older direct analyzer output from masking a
refreshed postprocess verdict.

`analysis/chassis_gt_contract/chassis_gt_contract_report.json` checks the GT
replacement contract for `/apollo/canbus/chassis`: channel count/rate/gaps,
timestamp/sequence monotonicity, chassis speed versus ego/localization speed,
driving mode, gear, error code, and available throttle/brake/steer feedback.
Missing chassis contract evidence is `insufficient_data`; it cannot be replaced
by a generic channel-health pass when making no-interference Apollo claims.
When the base `timeseries.csv` is intentionally compact, the analyzer may use
`artifacts/debug_timeseries.csv` as row-level chassis speed/time evidence. That
fallback can upgrade a placeholder `chassis_runtime_samples_missing` diagnosis
to a precise warn-level report, but it remains non-claim-grade if driving mode,
gear, error-code, or feedback fields are still absent.

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
- `artifacts/routing_event_debug.jsonl`
- `artifacts/planning_route_segment_debug.jsonl`
- `artifacts/control_decode_debug.jsonl`
- `artifacts/apollo_reference_line_contract.jsonl`
- `artifacts/apollo_hdmap_projection.jsonl`
- `artifacts/obstacle_gt_contract.jsonl`
- `artifacts/traffic_light_contract.jsonl`
- `artifacts/traffic_flow_events.jsonl`
- `artifacts/walker_spawn_candidates.jsonl`

Channel health analyzers should prefer
`analysis/channel_stats_normalized/channel_stats_normalized.json` when present.
The root-level `channel_stats.json` remains a compatibility copy. Counter-only
channels from `cyber_bridge_stats.json` prove runtime observation but are not
promotion-grade channel evidence unless row-level timestamped samples are also
present.

`artifacts/apollo_hdmap_projection.jsonl` has three different evidence states:
missing file is `artifact_missing`, existing zero-row file is `artifact_empty`,
and non-empty rows are `projection_rows_present`. Only rows with
`source=apollo_hdmap_api` can support claim-grade HDMap/reference-line evidence.
Rows whose `status` is `environment_unavailable` mean the exporter could not
reach the Apollo HDMap runtime, for example because the Apollo Docker container
or `map_xysl` command was unavailable. Treat that as `insufficient_data` and a
runtime/exporter wiring gap, not as proof of map alignment, lane direction, or
Planning algorithm failure.

The archive contains `package_manifest.json`, which records included files,
omitted large artifacts, missing required row-level evidence, and
`claim_reproducibility_level`. For typed transition runs it also records
`source_context_requirements` such as `examples/`, `configs/io/`,
`carla_testbed/runtime/`, `tbio/`, and `tools/apollo10_cyber_bridge/` so an
external review pack can include the source paths that produced the artifacts.
When row-level JSONL evidence is present, the
archive also contains `row_level_evidence_index.json` plus
`row_level_samples/**/*.head.jsonl` and `row_level_samples/**/*.tail.jsonl`.
The index records each sampled JSONL file's row count, size, sha256, time range,
and top-level fields so an external reviewer can sanity-check raw evidence
without unpacking large media or reading every row. Missing row-level evidence
keeps a claim package at summary-only review level; it must not be described as
claim-grade natural driving evidence.
The package manifest also separates `package_raw_evidence_complete` from package
file completeness. `sampled_artifacts` are audit samples only, and
`missing_full_artifacts` lists raw evidence required by the source run but not
present in the package/source directory. A package that contains only head/tail
samples is not full online evidence.

The row-level index is review scaffolding, not a pass condition by itself.
Analyzer reports still decide whether channel health, route contract, HDMap
projection, control attribution, prediction evidence, and natural-driving gates
are claim-grade.

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
If a compatibility runtime pre-created an `unknown` prediction placeholder,
link-health may recompute this layer from `channel_stats.json`, summary/
manifest scenario class, and the replacement matrix. That can make the boundary
explicit, for example `bypassed_with_gt_obstacles` for static lane-keep
diagnostics, but it still keeps `hard_gate_eligible=false` and blocks
closed-loop natural-driving claims unless native prediction or an explicit
scenario override exists.

`artifacts/control_apply_trace.jsonl` is the preferred row-level control-chain
evidence for Apollo CyberRT truth-input runs. Each row preserves Apollo raw
control, bridge mapped control, CARLA applied control, vehicle response,
latency, route progress, actuator mapping mode, calibration profile, steering
sign/scale, and apply-cadence diagnostics. It exists to make
raw -> mapped -> applied -> response attribution auditable; it must not be used
to hide raw Apollo command oscillation or to bypass localization/reference-line
failures.
Rows should also carry Apollo control header metadata under `apollo_control`,
including `header_sequence_num`, `header_timestamp_sec`, `rx_timestamp`, and
`control_timestamp` when available. `control_health_report.json` uses these
fields to distinguish repeated application/sampling of the same command from
new Apollo `/control` messages that really switch between throttle and brake.
If sequence evidence is missing, the oscillation diagnosis is still useful but
less claim-grade.
When simple_lon or trajectory-consume debug evidence is present,
`control_health_report.json` writes `control_semantics_primary_factor`,
`control_semantics_suspected_factors`, and `control_semantics_evidence`. These
summaries are designed for `apollo_link_health_report.json` and operator
triage; they are not a standalone root-cause proof and must not bypass missing
localization, HDMap projection, reference-line, or natural-driving evidence.
`metrics.control_oscillation_diagnosis` is the compact operator-facing roll-up
of the same evidence. It records raw-command oscillation presence,
same-trajectory switching, GT-state over-sampling, legacy/calibrated mapping
claim boundary, dominant source factors, and `next_debug_target`. It is an
artifact navigation aid; it does not convert a failed or incomplete run into a
natural-driving pass in `natural_driving_report.json`.
`metrics.control_decode_debug.planning_trajectory_correlation.transition_window_summary`
is the lower-level row navigation aid. It tells reviewers whether command
switches mostly occur inside the same consumed Planning sequence or at Planning
sequence updates, and provides small sample windows for the exact rows to
inspect.
If the trace exists but raw, mapped, and applied command fields are all null,
it is a no-command placeholder. That artifact is useful for loop/cadence
debugging, but it cannot satisfy raw/mapped/applied control evidence or a
natural-driving claim.

`artifacts/control_decode_debug.jsonl` and
`artifacts/bridge_control_decode.jsonl` remain fallback row-level Apollo control
decode evidence. They may supply Apollo raw command and bridge mapped command
layers when P0 `timeseries.csv` does not contain external-stack raw/mapped
fields or when `control_apply_trace.jsonl` is unavailable. They do not replace
CARLA applied-control evidence; mapped-to-applied and vehicle response checks
still need `control_apply_trace.jsonl`, applied-control timeseries, direct apply
rows, or vehicle response artifacts.

For Apollo lateral semantics and control attribution postprocess, prefer the
run-dir analyzers so they merge `artifacts/debug_timeseries.csv`,
`timeseries.csv`, `artifacts/planning_topic_debug.jsonl`, and
`config.resolved.yaml`. `debug_timeseries.csv` usually carries Apollo target /
matched point and raw/mapped command fields, while `timeseries.csv` carries
route curvature and cross-track evidence. A missing-field result from only one
of those files is diagnostic of an incomplete postprocess input, not proof that
the online run failed to record the field.
When present, `apollo_link_health_report.json` consumes
`analysis/apollo_lateral_semantics/apollo_lateral_semantics_report.json` as an
optional attribution layer. The layer is useful for selecting the next debug
question after HDMap projection and route cross-track agree on lateral drift;
it does not replace localization, HDMap projection, reference-line,
control-health, or natural-driving gates. The lateral report should include
`drift_window_summary` so operators can jump directly to the first-high and
max-lateral `route_s` windows and compare target/matched point, source steer,
mapped/applied steer, Apollo `simple_lat` lateral error, kappa, heading, and
yaw-rate context. In the aggregate link-health layer, compare
`cross_track_error_abs_p95` with
`apollo_simple_lat_lateral_error_abs_p95`: a high external route/HDMap lateral
error with near-zero Apollo `simple_lat` error is diagnostic of a semantic
closure problem, not evidence that natural-driving passed. Also compare
`route_s_vs_apollo_current_station_abs_delta_p95`; a large delta is acceptable
for local-control station frames, but it means `simple_lat` error should be
interpreted as local target/reference evidence rather than a route-level
lateral-health substitute. The lateral layer may also include
`ego_to_apollo_matched_point_xy_distance_p95` and
`route_to_apollo_matched_point_xy_distance_p95`; a near-zero ego-to-matched
distance with a high route-to-matched distance means Apollo's matched point is
tracking an ego-local/control reference while the configured route/HDMap
centerline remains laterally offset.
If `reference_debug_summary.reference_line_provider_ready_ratio` is low or
`reference_line_count_zero_ratio` is high while Planning trajectories are
non-empty, keep the run in reference/target semantics diagnosis. Non-empty
trajectory points are useful runtime evidence, but they do not replace a
claim-grade `apollo_reference_line_contract_report.json`.

`manifest.metadata.control_source=external_stack` is a harness-level label, not
claim-grade proof of Apollo control. `control_attribution_report.json` may
classify it as `/apollo/control` only when handoff / bridge artifacts prove the
`/apollo/control` channel plus nonzero rx/tx/apply counters. Explicit
non-Apollo sources such as route follower, builtin, manual, or direct autopilot
remain blocking even if Apollo control messages also exist.

Postprocess may regenerate `analysis/localization_contract/` only when strong
localization fields or bridge localization stats are present. Plain ego P0
timeseries can support route-health diagnostics, but it must not overwrite an
existing localization contract with weaker reconstructed evidence.

`artifacts/routing_event_debug.jsonl` is the row-level source for Apollo route
goal projection policy. Claim-grade route goals require
`goal_projection.accepted=true`, trusted lane-centerline source, snap distance
under `3m`, and lateral error under `1m`. An unaccepted or untrusted projection
that was applied is a route-contract failure, not a warning.

`analysis/natural_driving/natural_driving_report.json` now exposes
`planning_nonempty_ratio_for_claim` separately from filtered diagnostic ratios
such as `planning_nonempty_ratio_filtered_after_routing_segment_available`.
Only the claim-facing ratio can support a natural-driving pass.

`channel_stats.json.source.publish_gap_trace_summary` summarizes publish-gap
row evidence including writer/stats/publish-loop p95, async queue depth, and
artifact writer queue lag. Artifact backpressure keeps a run diagnostic until
fresh localization/chassis cadence recovers.

Duplicate `stage5_*` map/reference-line debug JSONL streams are optional
high-volume diagnostics. Online claim profiles may set
`bridge.stage5_debug_artifact_sample_stride` to sample those duplicate streams
while preserving the primary reference-line artifacts; the bridge records
sampled-out counts in `artifacts/cyber_bridge_stats.json`. Sampling those
duplicates does not relax required channel, localization, HDMap, reference-line,
or control artifacts.

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
also prints a causal blocker stack in the order
`goal -> map -> routing -> hdmap_projection -> planning -> control -> attribution`.
For example, a claim/materialization run with invalid scenario goal projection
and an invalid Apollo map contract should show
`first_blocker=goal_projection_unavailable/map_contract_invalid` before any
Planning or control symptom. This command does not replace detailed artifact
review for Apollo/Town01 capability promotion.

## P0/P1 Claim Evidence Refresh Rules

Postprocess may regenerate `localization_contract_report.json` and
`chassis_gt_contract_report.json` from `artifacts/cyber_bridge_stats.json` when
the bridge recorded canonical GT localization/chassis metadata. This fallback is
intended to prevent stale placeholder reports from hiding real runtime evidence.
It does not relax the claim gate: localization and chassis still need
`claim_grade=true`, no blocking reasons, and explicit frame / VRP / time-source
metadata before `natural_driving_report.json` can record a natural-driving pass.

`artifacts/apollo_hdmap_projection.jsonl` is different. An empty file is
reported as `empty_reason=apollo_hdmap_projection_artifact_empty_no_exported_rows`
with a next action to run `tools/export_apollo_hdmap_projection.py` against
Apollo's HDMap API. Empty or missing projection evidence keeps
`apollo_route_contract_report.json`, `apollo_reference_line_contract_report.json`,
and `natural_driving_report.json` non-claim-grade. CARLA waypoint projection,
nearest-lane debug rows, and Planning trajectory rows cannot substitute for
`source="apollo_hdmap_api"` projection samples.
When route/lane-equivalence coverage is being validated, export start/goal and
route samples explicitly:

```bash
python tools/export_apollo_hdmap_projection.py \
  --run-dir <run_dir> \
  --include-start-goal \
  --include-route-samples \
  --max-samples 500 \
  --min-route-s-coverage 50 \
  --out <run_dir>/artifacts/apollo_hdmap_projection.jsonl
```

The exporter status records `failure_reason`, requested route-s coverage,
observed `projection_s_coverage_m`, and non-ok projection status counts.

Single-run postprocess should still materialize
`analysis/natural_driving/natural_driving_report.json` and
`analysis/control_attribution/control_attribution_report.json` even when the run
fails. These reports are failure evidence, not a pass. A failing control
attribution report should be treated as secondary if route / HDMap /
reference-line contracts are still insufficient.

`analysis/gt_replacement_evidence/gt_replacement_evidence_report.json`
materializes the runtime view of `apollo_gt_replacement_matrix.yaml`. It lists
native, GT-replaced, CARLA-replaced, and bypassed modules; observed and missing
required evidence; per-module claim-grade status; and replacement blockers. It
is an audit surface, not a new pass condition.
