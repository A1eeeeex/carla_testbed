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
- Phase 1 backend input contract fields when available:
  - `backend_type`
  - `backend`
  - `backend_name`
  - `starts_runtime`
  - `starts_apollo`
  - `starts_autoware`
  - `input_contract`
  - `adapter_path`
  - `available_truth_fields`
  - `output_control_mode`
  - `transport_mode`
  - `fixed_scene_case`
  - `target_actor_contract`
  - `artifact_contract_version`
- `algorithm_variant_id` and `algorithm_variant_manifest_path` for any
  capability claim. The variant manifest must resolve inside the package and
  identify the same variant; missing or mismatched variant metadata keeps the
  run non-claim-grade.
- non-critical metadata such as smoke mode or runtime notes

Use the manifest to identify what the run claimed to execute. Do not use it as
a replacement for per-frame evidence. Phase 1 target actor fields are input
contract evidence only; v-t-gap and status reports still decide whether the run
is evaluable.

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
When available, `lane_invasion` event rows should include
`crossed_lane_marking_count` and `crossed_lane_marking_types`; without those
fields, downstream Baguang lane-event contracts may quarantine the event rather
than count it as a backend behavior loss.

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

## Phase 1 Scenario Comparison Artifacts

Phase 1 comparison does not require a natural-driving report. It requires
simple, backend-neutral artifacts that make target selection, longitudinal
response, and run validity explicit.

Additional run-local artifacts:

- `artifacts/fixed_scene_resolved.json`
  - should include `target_actor_contract` when the scenario has a target
    actor.
- `artifacts/scenario_actor_trace.jsonl`
  - fixed-scene actor state trace; used by `v-t-gap` extraction.
  - route-ahead fixed-scene runs may include `trajectory_progress_m`,
    `route_progress_gap_m`, and `route_progress_gap_source`; these are
    scenario-playback progress fields, not Apollo HDMap/Frenet evidence.
- `analysis/v_t_gap/v_t_gap_report.json`
  - schema: `v_t_gap.v1`
  - `status=not_applicable` is valid for route-only scenarios whose
    `target_actor_contract.status` is `not_required`, such as lane keeping,
    curve diagnostics, or junction turns. This means no target-gap metric
    applies; it is not a missing-target failure and not a following-behavior
    success.
- `analysis/v_t_gap/v_t_gap.csv`
  - fields include `sim_time_s`, `ego_speed_mps`, `target_speed_mps`,
    `gap_m`, `relative_speed_mps`, `target_actor_id`,
    `target_actor_role`, `gap_method`, `gap_degraded`, and
    `gap_degraded_reason`, `lateral_offset_m`,
    `longitudinal_center_gap_m`, `source_files`, `validity`, and
    `invalid_reason`.
  - route-aware rows may also include `ego_route_s`, `target_route_s`,
    `ego_lane_id`, `target_lane_id`, `route_s_direction_anchor_m`,
    `route_s_direction_anchor_source`, `route_s_direction_corrected`, and
    `route_gap_unavailable_reason`.
  - `gap_method=bumper_to_bumper_longitudinal_projection` is the preferred
    Phase 1 follow-target gap evidence and requires ego/target vehicle
    dimensions from runtime traces. For legacy Apollo compatibility runs, the
    extractor may use ego dimensions from
    `artifacts/carla_vehicle_characteristics.json` and target dimensions from
    `artifacts/scenario_actor_trace.jsonl` derived from obstacle GT contract
    rows. It still records the source files and does not infer a gap when the
    target actor contract is missing.
  - `gap_method=route_s_bumper_gap` is valid only when ego and target
    route/lane metadata are comparable, currently same `lane_id` plus ego and
    target vehicle lengths. Because CARLA lane-local `waypoint.s` may run
    opposite ego forward direction, the row records the direction anchor used
    to sign the route-s gap. If same-lane `route_s` disagrees materially with
    the actor-trace forward anchor, the extractor records
    `route_gap_unavailable_reason=route_s_anchor_conflict` and must fall back
    to trajectory-progress or another degraded method instead of reporting a
    false unsafe bumper gap. Missing or mismatched lane ids must stay
    degraded/diagnostic rather than being mixed into a false bumper gap.
  - `gap_method=trajectory_progress_bumper_gap` is a Phase 1 fixed-scene
    fallback for lane/road transitions where same-lane `route_s` is not
    comparable. It uses the initial observed center gap plus ego/target
    cumulative trajectory progress, and records fields such as
    `ego_trajectory_progress_m`, `target_trajectory_progress_m`,
    `trajectory_progress_initial_center_gap_m`, and
    `trajectory_progress_source`. This is observed scenario-playback gap
    evidence; it is not a substitute for an Apollo HDMap/Frenet projection
    contract in natural-driving claims.
  - `gap_method=bumper_to_bumper_longitudinal_projection_lateral_degraded`
    means the target was too far lateral for ego-frame longitudinal projection
    to be claim-grade bumper-gap evidence. Phase 1 status can report
    `degraded/degraded_gap_method`; it must not count a negative degraded
    projection as `failed/unsafe_gap`.
  - `gap_method=center_distance_fallback` or
    `existing_lead_gap_m_degraded` is diagnostic/degraded evidence, not a
    claim-grade bumper gap.
- `analysis/phase1_status/phase1_status.json`
  - status is one of `success`, `degraded`, `failed`, or `invalid`.
  - key fields include `run_id`, `scenario_case`, `backend_name`,
    `backend_type`, `status`, `failure_reason`, `evidence_files`,
    `invalid_reasons`, `degraded_reasons`, `failed_reasons`, and `notes`.
  - invalid runs are setup/artifact/config/backend-readiness problems and must
    not count as backend losses.
  - `phase1_metrics.lane_invasion_context` records the first timeseries row
    where `lane_invasion_count > 0`, including ego pose, speed, displacement
    from start, cross-track error, heading error, applied control, and whether
    the trigger was near the scenario start. This makes near-start
    lane-marking/spawn-footprint failures distinguishable from downstream
    behavior failures without changing the `failed/lane_invasion`
    classification.
  - if the run is otherwise evaluable and the runtime summary records
    `EGO_NOT_MOVING`, Phase 1 status maps it to `failed/stuck` before falling
    back to a generic `degraded/large_final_gap` classification. More specific
    failure reasons are used only when artifact evidence supports them:
    `failed/control_apply_mismatch` requires an authoritative source-command
    trace showing throttle while CARLA applied brake, while
    `failed/planning_control_handoff_missing` means planning-topic debug showed
    nonzero trajectory points but Apollo raw control debug input trajectory
    headers stayed zero, or the control stream ended before the first nonzero
    planning trajectory appeared. For Apollo external-stack runs, source command
    evidence is read from `artifacts/bridge_control_decode.jsonl` first, then
    `artifacts/apollo_control_raw.jsonl`, and only then from `timeseries.*`
    compatibility fields.
  - `phase1_metrics.control_motion` summarizes command/applied throttle and
    brake evidence. `command_source` records whether command evidence came
    from bridge decode, Apollo raw control, or timeseries fallback. If
    `timeseries.control_source` is mostly `external_stack`, `cmd_*` fields are
    treated as harness compatibility placeholders, not authoritative backend
    commands. A signature such as `source_brake_applied` means the bridge or
    Apollo raw trace commanded brake and CARLA applied brake; the run remains
    `failed/stuck`, not `control_apply_mismatch`. A signature such as
    `command_throttle_but_applied_brake` narrows a stuck run toward
    command/apply semantics, but it remains diagnostic evidence and does not
    change runtime behavior.
  - `phase1_metrics.control_motion.source_control_context` records compact
    upstream evidence from existing Apollo artifacts when available:
    `artifacts/bridge_control_decode.jsonl`,
    `artifacts/apollo_control_raw.jsonl`,
    `artifacts/control_trajectory_consume_debug_live.jsonl`,
    `artifacts/planning_topic_debug.jsonl`, and
    `artifacts/planning_route_segment_debug.jsonl`. For example,
    `source_brake_interpretation=planning_topic_nonzero_but_control_input_trajectory_zero`
    means planning-topic debug rows contained nonzero trajectory points while
    Apollo raw control debug input trajectory headers stayed zero. Phase 1 maps
    this narrow symptom to `failed/planning_control_handoff_missing`. It is a
    next-debug-action hint for planning/control handoff, not a root-cause proof
    and not a CARLA apply-layer failure by itself.
  - `phase1_metrics.derived_blocker_evidence` is an explanation layer assembled
    from run-local reports such as
    `analysis/control_health/control_health_report.json`,
    `analysis/apollo_link_health/apollo_link_health_report.json`, and
    `analysis/baguang_lane_event_contract/baguang_lane_event_contract_report.json`.
    It lets `phase1_status_summary.md` echo the current shortest-blocker view,
    for example an Apollo link-health primary blocker, a control-health failure
    reason, or a Baguang lane-event departure classification. This evidence
    does not reclassify the run, does not override `status` /
    `failure_reason`, and does not make a failed run claim-grade.
  - `analysis/apollo_control_handoff/apollo_control_handoff_report.json`
    additionally checks temporal overlap between nonzero planning rows and
    control output rows. The `planning_control_handoff` section records
    `first_planning_timestamp_sec`, `last_planning_timestamp_sec`,
    `first_nonzero_planning_timestamp_sec`,
    `last_nonzero_planning_timestamp_sec`, `first_control_timestamp_sec`,
    `last_control_timestamp_sec`, stream spans, and
    `planning_nonzero_rows_after_control_stream_end`. If `failure_reasons`
    contains `control_stream_ended_before_first_nonzero_planning`, first
    inspect Apollo control output/process survival and why control stopped
    producing commands before debugging trajectory consumption fields.
  - Phase 1 Apollo fixed-scene compatibility runs should use planning-ready
    deferred control start and write
    `artifacts/apollo_control_deferred_start.log` plus
    `artifacts/apollo_control_deferred_survival.json`. Missing survival evidence
    does not prove the backend failed, but it leaves the control-process
    lifecycle unverified and should be treated as a postprocess/online evidence
    gap before tuning control mapping.
    `run_phase1_postprocess()` ensures
    `analysis/apollo_control_handoff/apollo_control_handoff_report.json` for
    Apollo fixed-scene target runs before writing `phase1_status.json`. If that
    handoff report shows `failure_stage=process_health`, Phase 1 status maps
    the run to `failed/control_process_failed` rather than a generic stuck or
    no-control label.
    In the 2026-06-18 Baguang static follow-stop compatibility sample
    `runs/phase1_apollo_static_compat_bvar_disabled_20260618_110639`,
    `deferred_control_disable_bvar_dump=true` removed the bvar-dump log line
    as a confounder, but Control still exited with
    `process_health.crash_reason=tcmalloc_invalid_free` from
    `artifacts/apollo_control_deferred_mainboard.log`. Treat this as an Apollo
    Control process-survival blocker; it is not evidence of CARLA actuation,
    v-t-gap, or fixed-scene playback failure.
    The follow-up diagnostic overlay sample
    `runs/phase1_apollo_static_compat_control_overlay_20260618_112758`
    staged 17 historical control runtime files and advanced the same scenario
    to `apollo_control_handoff.verdict=warn` with
    `failure_stage=none`; `/apollo/control`, bridge receive, decode, mapping,
    apply, and vehicle response evidence all materialized. That run still
    failed Phase 1 as `failed/lane_invasion`, so the overlay is diagnostic
    process-survival evidence only. Do not promote it to default runtime or
    treat it as an Apollo behavior pass without repeated no-regression evidence.
  - Phase 1 Apollo compatibility configs may set
    `runtime.postprocess.auto_export_apollo_hdmap_projection=true`. In that
    mode the runtime attempts a best-effort Apollo `map_xysl` export before
    the Phase 1 postprocess spine analyzes the run. The resulting
    `artifacts/apollo_hdmap_projection.jsonl`,
    `analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json`, and
    `analysis/apollo_hdmap_projection_export/apollo_hdmap_projection_export_status.json`
    are HDMap projection evidence only. Export success does not change driving
    behavior, does not repair lane invasion, and does not make a diagnostic
    compatibility run claim-grade.
    `analysis/apollo_lateral_semantics/apollo_lateral_projection_pairing.csv`
    may be emitted when lateral-semantics analysis can time-align sparse
    `timeseries.*` rows with official `apollo_hdmap_api` projection rows. The
    CSV is a row-level audit surface for signs and magnitudes of
    `cross_track_error`, Apollo `simple_lat` lateral error, and
    HDMap `projection_l`. When `localization_x/y` and `lane_heading_at_s` are
    present, it also records the lane-center projection point reconstructed
    from `projection_l`; this is official HDMap projection geometry, not
    materialized scenario route geometry, and it does not prove ego behavior
    success.
    `run_phase1_postprocess()` refreshes the dependent reports in evidence
    order: Apollo HDMap projection, Apollo route contract, reference-line
    contract, module-consumption, then link-health. This prevents stale route
    contract output from downgrading a newer projection/reference-line analysis,
    but any warning or blocker from the refreshed route contract still
    propagates into later claim gates.
  - fixed-scene playback validity includes both
    `analysis/fixed_scene_contract/fixed_scene_contract_report.json` and
    `analysis/scenario_actor_contract/scenario_actor_contract_report.json`.
    If a non-ego scenario actor fails its scripted behavior contract, for
    example a lead vehicle cannot execute the requested speed profile, the run
    is `invalid/fixed_scene_failed` even if `v_t_gap` contains an unsafe gap.
    The unsafe gap is diagnostic evidence, not a backend loss, until the fixed
    scene actor behavior itself is valid.
  - Apollo fixed-scene target-actor runs require obstacle GT contract evidence
    that the scenario target actor reached Apollo perception. If
    `backend_type=apollo_reference_backend` and the target actor contract is
    resolved, missing `analysis/obstacle_gt_contract/obstacle_gt_contract_report.json`
    or `artifacts/obstacle_gt_contract.jsonl` is classified as
    `invalid/missing_apollo_obstacle_gt_contract`, not as an Apollo behavior
    failure.
  - The preferred fixed-scene obstacle linkage source is
    `fixed_scene_runtime_state.actor_roles`. If an Apollo bridge run only has
    row-level obstacle evidence, `obstacle_gt_contract.jsonl` may link the
    target by fields such as `front_obstacle_actor_role=lead_vehicle` plus
    `carla_actor_id`; the report records
    `actor_role_source=obstacle_gt_contract_record_roles` and a warning so the
    provenance is visible.
- `analysis/artifact_completeness/artifact_completeness_report.json`
  - records the canonical Phase 1 or natural-driving artifact completeness
    profile consumed by ScenarioComparison and acceptance gates.
- `analysis/phase1_status/artifact_completeness.json`
  - legacy Phase 1 echo file kept for older postprocess consumers.
  - echoes `artifact_contract_version`, `target_actor_contract`, and
    `target_actor_contract_status` for auditability. This echo does not by
    itself make a run valid; `phase1_status.json` still classifies invalid,
    degraded, failed, or successful outcomes.

`carla_builtin` fixed-scene and route-only diagnostic runs invoke the Phase 1
postprocess spine after runtime completion. The postprocess writes
`analysis/v_t_gap/`, `analysis/phase1_status/`, canonical
`analysis/artifact_completeness/`, and the legacy Phase 1 artifact-completeness
echo without changing control behavior. When postprocess is rerun, it refreshes
the Phase 1 echo fields in an existing `summary.json`, such as
`phase1_status`, `phase1_failure_reason`, `v_t_gap_status`, and
`artifact_completeness_status`; the authoritative Phase 1 classification
remains `analysis/phase1_status/phase1_status.json`. Route-only runs do not require
`fixed_scene_resolved.json`, `fixed_scene_runtime_state.json`,
`scenario_actor_trace.jsonl`, or `scenario_phase_events.jsonl` when the target
actor contract is explicitly `not_required`.

`carla_builtin` online runs also attach CARLA collision and lane-invasion
sensors to the ego vehicle when the local runtime supports them. The run
summary and `timeseries.csv` expose `collision_count`, `lane_invasion_count`,
`collision_sensor_available`, and `lane_invasion_sensor_available`; individual
events are written to `artifacts/safety_event_trace.jsonl`. If sensor creation
fails, the run can still complete as diagnostic playback, but
`phase1_status.phase1_metrics.safety_event_evidence` marks the corresponding
surface unavailable so ScenarioComparison cannot use that safety event as a
cross-backend behavior loss.

Apollo compatibility runs may also include
`analysis/channel_cadence_diagnosis/channel_cadence_diagnosis_report.json`.
This report distinguishes wall-clock delivery cadence from Apollo header
sim-time cadence. Its `sim_wall_cadence` block records CARLA sim elapsed time,
wall elapsed time, the sim/wall speedup factor, and whether wall-time pacing was
enabled. If sim-time advances much faster than wall-time while pacing is
disabled, the report should keep the channel gate failing but set
`primary_gap_source=sim_time_speedup_without_wall_pacing`; the next validation is
a paced diagnostic run, not control smoothing or PID tuning.

Legacy Apollo online runs can be explicitly bound to a Phase 1 fixed-scene
scenario after the fact when the operator knows which `ScenarioCase` was
intended:

```bash
python tools/bind_phase1_scenario_run.py \
  --run-dir <apollo_run> \
  --scenario configs/scenarios/town01/follow_stop_097.yaml \
  --role-alias lead_vehicle=front \
  --postprocess
```

This writes `analysis/phase1_scenario_binding/phase1_scenario_binding_report.json`,
updates `manifest.json` with the Phase 1 scenario identity, and writes
`artifacts/fixed_scene_resolved.json` from the tracked scenario YAML. Role
aliases are explicit operator declarations, for example mapping a legacy
bridge row role `front` to the Phase 1 canonical role `lead_vehicle`.
For Apollo legacy runs, postprocess readiness prefers the runtime
`artifacts/cyber_bridge_stats.json` `front_obstacle_behavior` block when no
resolved bridge config path is available. That checks the bridge role names
and actor-probe setting observed in the run, while still marking
operator-declared role aliases as warning-level setup evidence.
If a legacy Apollo run records `phase1_scenario_path` in `manifest.json`,
`summary.json`, `config.resolved.yaml`, or `typed_runtime.effective_legacy.yaml`,
`run_phase1_postprocess()` can perform this binding automatically. For legacy
bridges that publish the target role as `front`, postprocess maps it to the
Phase 1 canonical `lead_vehicle` role only as explicit setup evidence; obstacle
GT, `v-t-gap`, handoff, and status reports still decide whether the run is
evaluable.

Binding is setup evidence only. It does not verify that fixed-scene playback
actually followed the storyboard, and it does not turn a legacy Apollo smoke
into backend behavior success. `phase1_status.json`, `v_t_gap_report.json`,
`obstacle_gt_contract_report.json`, artifact completeness, and
`ScenarioComparison` still decide whether the run is evaluable.

Comparison directory artifacts:

- `comparison_manifest.json`
- `comparison_summary.json`
  - schema: `phase1_comparison.v1`
  - includes `comparison_id`, `scenario_case`, `participating_runs`,
    `backends`, `comparison_status`, `comparison_target_status`,
    `backend_coverage`, `invalid_runs`, `backend_results`, `v_t_gap_files`,
    `summary_notes`, and `evidence_files`.
  - when present, run entries and backend results echo
    `apollo_control_handoff_status`,
    `apollo_control_handoff_failure_stage`, and
    `apollo_control_handoff_blocking_reasons` from
    `analysis/apollo_control_handoff/apollo_control_handoff_report.json`.
    The Markdown table also shows the handoff stage so an Apollo backend loss
    can be separated from target/gap/playback failures without opening every
    nested artifact.
  - when present, backend results also carry
    `phase1_metrics.derived_blocker_evidence` from each run. The Markdown table
    may surface compact fields such as link-health primary blocker,
    control-health failure reason, and lane-event departure class. These fields
    explain an evaluable run-level failure; they do not change whether the run
    is evaluable, do not convert an invalid run into a backend loss, and do not
    declare an Apollo natural-driving pass; any such claim still requires a
    matching `natural_driving_report.json`.
  - `scenario_case` is the comparison identity. Legacy online runs may carry
    older or generic `scenario_id` values; if they normalize to the same
    `scenario_case`, comparison may proceed while preserving the original
    values in `scenario_ids` for auditability.
  - `comparison_target_status=apollo_vs_planning_control_evaluable` is the
    Phase 1 target comparison shape. Same-backend comparisons are valid
    regression evidence but are labeled `same_backend_regression`, not Phase 1
    ApolloBackend-vs-PlanningControlBackend completion.
  - if a participating run declares blocking assists such as a route follower,
    dummy lateral control, straight acceleration override, or lateral
    stabilizer, the run may remain evaluable as assisted scenario evidence but
    the comparison target is not `apollo_vs_planning_control_evaluable`; failed
    assisted runs must not be counted as reference-backend losses.
  - safety-event failures require a matching safety-event evidence surface
    across participating evaluable runs. `phase1_status.phase1_metrics` now
    includes `safety_event_evidence.collision` and
    `safety_event_evidence.lane_invasion`, each recording whether the summary
    or `timeseries.*` exposes the corresponding counter. If one backend fails
    with `lane_invasion` or `collision` and another evaluable backend lacks
    the same event counter surface, `ScenarioComparison` must report
    `comparison_status=partially_evaluable` and
    `comparison_target_status=safety_event_evidence_mismatch`; the safety
    failure remains a run-level `failed/*`, but it is not counted as a
    cross-backend loss.
  - if every participating evaluable backend fails with lane invasion within
    the first few meters while `lane_invasion_context` shows low cross-track
    and heading error, `ScenarioComparison` reports
    `comparison_target_status=shared_safety_event_context_issue`. The run-level
    outcome remains non-pass, but this shared near-start lane-event context is
    treated as a scenario/map/sensor contract issue rather than a backend
    behavior loss. For Baguang, `baguang_lane_event_contract_report.json`
    should be used to support or reject this quarantine boundary. That report
    should prefer run-specific ego footprint evidence from
    `timeseries.bbox_extent_y_m`, `timeseries.ego_width_m`, or
    `artifacts/carla_vehicle_characteristics.json` before falling back to a
    default half-width. When `artifacts/safety_event_trace.jsonl` includes
    `crossed_lane_marking_types`, the contract should also report whether the
    crossed marking types match the parsed target-lane `roadMark` types; a
    mismatch is diagnostic map/sensor evidence, not a backend control result.
    If an offset sweep is available, it should be recorded in the contract as
    `offset_sweep`. A result such as `road_start_only_trigger=true` means the
    lane event is localized to the road-start/spawn neighborhood and must not
    be counted as a backend behavior loss until the map/sensor contract is
    repaired or the scenario start is shifted and revalidated. The
    `follow_stop_static_300m_spawn2m` diagnostic variant records
    `manifest.ego_spawn_s_offset_m=2.0`; the 2026-06-18 builtin validation
    runs `runs/phase1_baguang_follow_stop_static_spawn2m_builtin_20260618_140307`
    and
    `runs/phase1_baguang_follow_stop_static_spawn2m_builtin_20260618_140340_30s`
    both produced `lane_invasion_count=0` and
    `baguang_lane_event_contract.status=pass`. This proves the road-start
    mitigation for the builtin diagnostic backend only. The Apollo LaunchPlan
    can dispatch the matching shifted-start compatibility config
    `configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_compat.yaml`,
    and the 2026-06-18 online sample
    `runs/phase1_baguang_apollo_followstop_static_spawn2m_compat_20260618_142848`
    proved the offset reached runtime metadata:
    `artifacts/scenario_metadata.json.ego_spawn_s_offset_m=2.0`,
    `front_alignment.longitudinal_m≈300m`,
    `lane_invasion_count=0`, `collision_count=0`,
    `obstacle_gt_contract.status=pass`, `v_t_gap.status=pass`,
    and `baguang_lane_event_contract.status=pass`. That run is evaluable but
    failed with `phase1_status=failed/control_process_failed` and
    `apollo_control_handoff.failure_stage=process_health`, so it is an
    ApolloBackend process-health failure rather than a spawn/target/gap invalid
    run or an Apollo behavior success.
    The follow-up explicit overlay sample
    `runs/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_compat_20260618_145220`
    used
    `configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_compat.yaml`
    and wrote `artifacts/apollo_control_runtime_overlay_manifest.json` with
    `selected_count=17`. It restored `apollo_control_handoff` to
    `warn/failure_stage=none` and made `/apollo/control` materialize, but the
    run then failed as `phase1_status=failed/lane_invasion` after about 55.6m
    of Apollo-controlled motion. The refreshed
    `baguang_lane_event_contract_report.json` records
    `departure_diagnostics.classification=downstream_progressive_lane_departure`
    with cross-track and heading error growing before the event. The same block
    now prefers `artifacts/control_apply_trace.jsonl` over compatibility
    timeseries control placeholders. In that authoritative trace, Apollo raw
    steer grows to about `-0.159`, mapped and CARLA-applied steer both reach
    about `-0.0396`, and
    `mapped_to_applied_steer_abs_error.max_abs_error=0.0`. The same report
    records `raw_to_mapped_steer_gain.mean≈0.244`, final gain `0.25`, and
    Apollo raw steer sharing the cross-track-error sign. This is diagnostic
    process-survival evidence plus an ApolloBackend lateral-behavior /
    source-steer / legacy-mapping-semantics attribution failure; it is not an
    Apollo behavior pass, not a CARLA apply mismatch proof, and the overlay
    remains non-default.
    The refreshed `analysis/control_health/control_health_report.json` now
    treats stale summary fields such as `routing_materialized=false`,
    `planning_materialized=false`, and
    `control_handoff_status=planning_not_materialized` as lower-priority than
    explicit routing response, planning-topic, and control-handoff artifacts.
    For this run it records `routing_materialized=true`,
    `planning_materialized=true`,
    `control_handoff_status=control_consuming_with_nonzero_planning`, and
    `failure_reason=apollo_raw_command_oscillation`, with warnings that the
    summary fields were overridden by stronger artifacts. This fallback is an
    evidence-consistency repair only; it does not hide explicit
    `control_missing` summaries and it does not turn the Apollo run into a
    behavior success.
    To compare native control-process failures with a diagnostic overlay run,
    use `tools/analyze_apollo_control_runtime_diff.py`. It writes
    `analysis/apollo_control_runtime_diff/apollo_control_runtime_diff_report.json`
    and a markdown summary. The report compares overlay targets, control
    survival, handoff crash signatures, and Apollo docker library manifests.
    A positive diff is diagnostic runtime evidence only: it can show that a
    control-runtime overlay correlates with process survival, but it does not
    make the overlay default and does not count as Apollo natural-driving
    success.
    Exact-target smoke probes in
    `artifacts/apollo_control_runtime_smoke_exact_20260618_204258/` can narrow
    the suspect runtime files without running CARLA, but their result is weaker
    than a full online run. In the 2026-06-18 probes,
    `liblat_controller.so` alone and the three controller libraries passed the
    isolated control-DAG smoke, yet the corresponding online runs
    `runs/phase1_baguang_apollo_spawn2m_lat_overlay_20260618_205014` and
    `runs/phase1_baguang_apollo_spawn2m_controller_overlay_20260618_210017`
    still ended with `NO_CONTROL_LOG`. Larger online bisection variants also
    failed: `runs/phase1_baguang_apollo_spawn2m_proto_only_overlay_20260618_220859`
    staged only `lib_control_cmd_proto_mcc_bin.so` and still reported
    `apollo_control_handoff.failure_stage=process_health`,
    `crash_reason=tcmalloc_invalid_free`, and no control survival at 5s/10s.
    That proto-only run is `phase1_status=invalid` because its obstacle GT
    contract failed, so it is not a backend behavior loss; it remains valid as
    read-only control-runtime bisection evidence. The run
    `runs/phase1_baguang_apollo_spawn2m_controllers_submodules_overlay_20260618_211422`
    staged 13 controller/submodule files, and
    `runs/phase1_baguang_apollo_spawn2m_no_controlmsgs_overlay_20260618_212342`
    staged the full set minus `lib_control_cmd_proto_mcc_bin.so`; both remained
    `apollo_control_handoff.failure_stage=process_health` /
    `NO_CONTROL_LOG`. A positive-control rerun,
    `runs/phase1_baguang_apollo_spawn2m_full_overlay_rerun_20260618_213330`,
    staged the full 17-file set and restored
    `apollo_control_handoff.verdict=warn`, `failure_stage=none`, and bridge
    `control_rx_count/control_tx_count=9231/9231`, but still failed Phase 1 as
    `failed/lane_invasion`. The machine-readable bisection summary is
    `artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/online_bisect_summary.json`.
    The focused 16-vs-17 report
    `artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/runtime_diff_16_vs_17/apollo_control_runtime_diff_report.json`
    sets
    `primary_difference=control_cmd_proto_runtime_target_correlates_with_process_survival`
    and records `single_added_name=lib_control_cmd_proto_mcc_bin.so`. The
    follow-up diffs
    `artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/runtime_diff_no_overlay_vs_proto_only/`
    and
    `artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/runtime_diff_proto_only_vs_full_17/`
    show that the proto library alone does not improve survival; the remaining
    16 control runtime files are still required in the current online
    diagnostic condition.
    The
    read-only provenance report
    `artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/control_cmd_proto_provenance/control_cmd_proto_provenance.json`
    compares the overlay source with the current restored target inside the
    Apollo container. It records different hashes
    (`689c6e8d6...` vs `8c210ec8...`), different sizes (about `0.9MB` vs
    `2.2MB`), and different ELF path style (`RPATH` vs Bazel-style `RUNPATH`).
    This supports a control-command protobuf runtime provenance/ABI hypothesis;
    it is still read-only diagnostic evidence, not permission to patch Apollo
    binaries or make the overlay default.
    Reproduce the provenance report with:
    `python tools/analyze_apollo_control_runtime_provenance.py --overlay-manifest runs/phase1_baguang_apollo_spawn2m_full_overlay_rerun_20260618_213330/artifacts/apollo_control_runtime_overlay_manifest.json --record-name lib_control_cmd_proto_mcc_bin.so --container apollo_neo_dev_10.0.0_pkg --source-run runs/phase1_baguang_apollo_spawn2m_full_overlay_rerun_20260618_213330 --baseline-run runs/phase1_baguang_apollo_spawn2m_no_controlmsgs_overlay_20260618_212342 --online-diff-report artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/runtime_diff_16_vs_17/apollo_control_runtime_diff_report.json --out artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/control_cmd_proto_provenance`.
    The set-level provenance report
    `artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/full_17_overlay_set_provenance/apollo_control_runtime_overlay_set_provenance_report.json`
    compares every source/target pair in the 17-file overlay manifest. It
    records `existing_pair_count=17`, `same_content_count=0`,
    `different_content_count=17`, `records_with_elf_path_style_diff=13`, and
    `records_with_needed_libraries_diff=12`. This narrows the current evidence
    away from a single-file proto explanation and toward a runtime-set
    ABI/configuration mismatch hypothesis. The report remains diagnostic-only:
    it does not make the overlay default, does not prove Apollo behavior
    success, and should not be used as a capability claim.
    Reproduce the set-level provenance with:
    `python tools/analyze_apollo_control_runtime_provenance.py --all-records --overlay-manifest runs/phase1_baguang_apollo_spawn2m_full_overlay_rerun_20260618_213330/artifacts/apollo_control_runtime_overlay_manifest.json --container apollo_neo_dev_10.0.0_pkg --source-run runs/phase1_baguang_apollo_spawn2m_full_overlay_rerun_20260618_213330 --baseline-run runs/phase1_baguang_apollo_spawn2m_no_controlmsgs_overlay_20260618_212342 --online-diff-report artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/runtime_diff_proto_only_vs_full_17/apollo_control_runtime_diff_report.json --out artifacts/apollo_control_runtime_overlay_online_bisect_20260618_212000/full_17_overlay_set_provenance`.
    The
    current inference is that this control command protobuf runtime target in
    combination with the other 16 files is part of the online control
    materialization condition. This remains a diagnostic runtime differential;
    smaller subsets are not behavior evidence, and the full overlay must not
    be promoted to default without separate no-regression evidence.
  - if an Apollo run is present but invalid, `backend_coverage` keeps the
    Apollo backend visible while `comparison_target_status` reports that there
    is no evaluable Apollo reference backend.
- optional `comparison_curves/`
  - when present, the generic Phase 1 comparison writer emits
    `comparison_curves/v_t_gap_combined.csv`.
  - the combined curve includes evaluable runs only. Invalid runs remain in
    `comparison_manifest.json` and `comparison_summary.json`, but they are not
    merged into behavior curves and cannot be counted as backend losses.

`comparison_status=comparable` is allowed only when participating runs are
evaluable. If any run is invalid, the comparison is `partially_evaluable` or
`invalid`; it must not declare a backend winner from invalid setup evidence.

`tools/run_phase1_scenario.py` can create a CI-safe Phase 1 scaffold for
backend readiness checks. For `apollo_cyberrt` fixed-scene scenarios it writes
`manifest.json`, `preflight.json`, `summary.json`, and
`analysis/phase1_status/phase1_status.json` with
`status=invalid/failure_reason=backend_not_ready` when the fixed-scene Apollo
runtime is not migrated. That scaffold is ingestible by ScenarioComparison, but
it is not online behavior evidence and must not count as an Apollo backend loss.
The scaffold may also write static-only fixed-scene compile artifacts such as
`artifacts/fixed_scene_resolved.json`,
`analysis/fixed_scene_contract/fixed_scene_contract_report.json`, and
`analysis/scenario_actor_contract/scenario_actor_contract_report.json`. These
reports can prove the ScenarioCase is structurally compilable, but they remain
`insufficient_data` without runtime actor trace and phase events. An online
Apollo fixed-scene run additionally needs fixed-scene runtime state, scenario
actor trace, CyberRT/bridge artifacts, and obstacle GT contract evidence for
the target actor before `phase1_status` can evaluate backend behavior.

For Apollo compatibility runs that already produce row-level
`artifacts/obstacle_gt_contract.jsonl`, Phase 1 postprocess may derive
`artifacts/fixed_scene_runtime_state.json` and
`artifacts/scenario_actor_trace.jsonl` from observed fields such as
`front_obstacle_actor_role`, `carla_actor_id`, position, speed, and
front-obstacle gap. The derived files are marked
`derived_from_apollo_obstacle_gt_contract` and are valid only as compatibility
evidence that a target actor was observed in Apollo obstacle/probe rows. They
may be preserved on repeated postprocess runs and reported as
`artifact_sources.*=preserved_existing`; this is idempotent evidence reuse, not
a new runtime observation.
do not by themselves prove the target actor was published in Apollo perception
obstacles, do not prove fixed-scene playback success, and do not override
`backend_not_ready`, missing CyberRT artifacts, or failed obstacle contracts
such as `fixed_scene_actor_probe_not_published_to_apollo_obstacles`.

Apollo fixed-scene scaffolds may also write
`analysis/phase1_apollo_fixed_scene_readiness/phase1_apollo_fixed_scene_readiness_report.json`.
This is a run-preflight/config-readiness artifact. It checks whether the Apollo
bridge `front_obstacle_behavior` configuration can probe the fixed-scene target
actor role and therefore produce row-level obstacle evidence for the target.
For example, a Baguang follow-stop target role of `lead_vehicle` is not ready if
the bridge role list only contains `front`, or if actor probing is disabled.
Readiness pass/warn/fail is not behavior evidence; it only explains whether an
online Apollo fixed-scene run is configured to make the target actor visible in
auditable obstacle rows.
`run_phase1_postprocess()` also writes this report automatically for Apollo
fixed-scene target runs, so archived run directories can be refreshed without
rerunning CARLA or Apollo.

Operator preflight for a resolved or candidate bridge config:

```bash
python tools/analyze_phase1_apollo_fixed_scene_readiness.py \
  --scenario configs/scenarios/baguang/follow_stop_static_300m.yaml \
  --bridge-config <resolved_apollo_bridge_config.yaml> \
  --out <run>/analysis/phase1_apollo_fixed_scene_readiness
```

For the shifted-start diagnostic variant, use:

```bash
python tools/analyze_phase1_apollo_fixed_scene_readiness.py \
  --scenario configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml \
  --bridge-config <resolved_apollo_bridge_config.yaml> \
  --out <run>/analysis/phase1_apollo_fixed_scene_readiness
```

The Phase 1 scaffold can also record the same bridge config path in
`manifest.json` / `preflight.json` and use it for readiness:

```bash
python tools/run_phase1_scenario.py \
  --scenario configs/scenarios/baguang/follow_stop_static_300m.yaml \
  --backend apollo_cyberrt \
  --bridge-config configs/io/examples/phase1_baguang_apollo_fixed_scene_bridge.yaml \
  --run-dir runs/<apollo_fixed_scene_preflight>
```

`configs/io/examples/phase1_baguang_apollo_fixed_scene_bridge.yaml` is a
Phase 1 readiness example that enables actor probing and includes
`lead_vehicle` in `front_obstacle_behavior.role_names`. It still does not start
Apollo or prove online behavior.

Apollo fixed-scene scaffolds and postprocess may also write
`analysis/phase1_apollo_fixed_scene_dispatch/phase1_apollo_fixed_scene_dispatch_report.json`.
This is a LaunchPlan contract check, not online evidence. It separates:

- `dispatch_mode=guarded_legacy_transition_available`: a narrow static
  Baguang follow-stop compatibility command exists, but must still be run and
  postprocessed before any behavior comparison.
- `dispatch_mode=runtime_migration_required`: dynamic lead accel/decel,
  cut-in/cut-out, and hard-brake cases still need Apollo runtime migration
  before they can be evaluable.

For `runtime_migration_required`, the report also writes
`runtime_migration_requirements`. These requirements are a concrete handoff
list for the next runtime PR, for example online `CarlaFixedSceneRuntime`
startup, speed-profile or lane-change playback for non-ego actors,
`scenario_actor_trace.jsonl`, `scenario_phase_events.jsonl`, obstacle GT role
linkage, and `v-t-gap` extraction. They are not pass evidence; they explain
why the ScenarioCase remains invalid/partial for ApolloBackend.

Inspect dispatch readiness without starting CARLA/Apollo:

```bash
python tools/analyze_phase1_apollo_fixed_scene_dispatch.py \
  --scenario baguang_lead_decel_70_to_40_20m \
  --out <run>/analysis/phase1_apollo_fixed_scene_dispatch
```

Generate CI-safe Apollo dispatch scaffolds for the remaining dynamic
fixed-scene cases without starting runtime:

```bash
python tools/run_phase1_apollo_dispatch_scaffold_batch.py \
  --out runs/<phase1_apollo_dynamic_dispatch_scaffolds>
```

The batch writes `phase1_apollo_dispatch_scaffold_batch_manifest.json` and
`phase1_apollo_dispatch_scaffold_matrix.csv`. Rows with
`phase1_status=invalid` and `dispatch_mode=runtime_migration_required` are
expected until the Apollo online runner is migrated to start fixed-scene actor
playback. They are setup evidence and must not count as Apollo backend losses.

Scenario catalog artifacts:

- `phase1_scenario_catalog.json`
- `phase1_scenario_catalog.md`

The catalog is a review/orchestration artifact, not run evidence by itself. It
lists each P0/P1 case with separate `case_yaml_status`, `template_status`,
`carla_online_status`, `apollo_online_status`, `v_t_gap_status`, and
`comparison_status` / `comparison_target_status` fields. For fixed-scene Apollo
runs it also surfaces `apollo_fixed_scene_readiness_status` and
`apollo_fixed_scene_readiness` when the readiness report is present,
`apollo_fixed_scene_dispatch_contract` / `apollo_fixed_scene_dispatch_mode`
when the LaunchPlan dispatch report is present, and
`apollo_fixed_scene_runtime_dispatch_status` /
`apollo_fixed_scene_runtime_dispatch_reason` when Apollo preflight/runtime
artifacts show whether fixed-scene dispatch actually executed. This keeps
bridge-config readiness, dispatch-contract evidence, and runtime migration
separate: a readiness `pass` with a missing dispatch report,
`dispatch_mode=runtime_migration_required`, or
`apollo_fixed_scene_runtime_not_migrated` is still only scaffold/setup evidence.
Missing actor-probe, role-name mismatch, or runtime-dispatch absence remains a
setup/evidence blocker rather than being counted as an Apollo behavior loss. It
also records `scenario_case_ids`, `case_files`, and `target_actor_contract` so
reviewers can trace which concrete ScenarioCase and target role support each
readiness row. A fixed-scene scenario with online runs but missing Apollo
dispatch-contract evidence must remain `PARTIAL`; a scenario with
YAML/template evidence but missing online or target-comparison evidence must
remain `PARTIAL` or `NOT_YET`, never `DONE`.
`tools/phase1_scenario_catalog.py` supports `--evidence-root` for review packs
or copied run evidence; without it, the catalog scans `<repo>/runs`.
When the selected representative comparison carries
`phase1_metrics.derived_blocker_evidence`, the catalog may echo compact
link-health, control-health, and lane-event blocker summaries. These summaries
are review navigation hints only; `phase1_status.json` and
`comparison_summary.json` remain the authoritative run/comparison outcomes.
When a representative Apollo fixed-scene dispatch report carries
`runtime_migration_requirements`, the catalog echoes the first requirements in
`next_action` and the dispatch evidence details. This is a migration checklist,
not proof that dynamic fixed-scene runtime dispatch has executed.

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
- For fixed-scene follow-target runs, `artifacts/scenario_metadata.json` may
  contain runtime `spawn`, `front_spawn`, and `front_alignment` evidence. If
  `front_alignment.aligned=true`, postprocess may derive a two-point
  fixed-scene route trace and CARLA lane hint from runtime timeseries
  `dbg_ref_*` fields. That route trace is scenario intent evidence only; it
  does not prove Apollo consumed the same lane, and it still requires official
  Apollo HDMap projection rows for lane-equivalence claims.
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
- `artifacts/map_identity_report.json`
- `artifacts/lane_equivalence_town01.json`
- `analysis/apollo_map_route_alignment/apollo_map_route_alignment_report.json`

`map_identity_report.json` records the observed Apollo map root candidates,
bridge effective map file, Dreamview selected map when available, projection
exporter map dir, and base/routing/sim map hashes. `lane_equivalence_town01.json`
records ordered CARLA-route and Apollo-routing lane signatures; it must not
pass from unordered set overlap or lane-id string equality alone.
`apollo_map_route_alignment_report.json` separates `static_map_identity`,
`runtime_projection`, `routing_response`, and `planning_reference_line` layers,
then emits diagnoses such as `projection_missing`, `projection_geometry_bad`,
`lane_equivalence_missing`, `boundary_transition_ambiguous`,
`heading_convention_mismatch`, `routing_not_claim_route`, or
`reference_line_missing`. This is still a precondition report, not a closed-loop
natural-driving pass.
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
Official route projection rows can also carry
`route_trace_heading_error_rad`, `route_chord_heading_error_rad`, and
`route_heading_source`. These fields let the lane-equivalence artifact keep raw
route-trace heading diagnostics while evaluating short connector and junction
transition lanes from core/chord evidence. A boundary sample may become a
warning instead of a blocker only when the non-boundary route samples still
prove dominant lane, lateral, heading, and ordered RoutingResponse consistency.
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
`planning_runtime_messages_missing` or `apollo_module_runtime_logs_missing`.
Link-health may also recompute an Apollo module-consumption report whose only
failure reasons were route-contract-derived stale blockers, for example
`route_contract_unverified_before_module_consumption_claim` or
`claim_route_consumption_unverified`, after the route-contract artifact has
been refreshed. A missing report remains missing, and a real semantic `fail`
such as input timeout logs or persistent reference-line provider failure is not
overwritten.
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
manifest scenario class, Apollo module-status logs, and the replacement
matrix. That can make the boundary explicit, for example
`bypassed_with_gt_obstacles` for static lane-keep diagnostics or `missing` when
Prediction runtime was observed but no `/apollo/prediction` output was
observed. Static lane-keep bypass evidence must not be generalized to dynamic,
junction, or traffic-light scenarios; those remain blocked unless native
prediction or an explicit scenario-scoped override exists.
For Apollo transition/CyberRT runs, the review package should also include
`artifacts/apollo_prediction.INFO` when available, alongside
`artifacts/apollo_log_capture_meta.json` and
`artifacts/apollo_log_snapshot_meta.json`. If the Prediction mainboard was
observed but `/apollo/prediction` has zero messages, this log snapshot is the
first artifact to inspect for initialization, input, or runtime errors. Missing
Prediction logs do not make the run pass; they leave the root cause less
observable.
`prediction_evidence_report.json` may also report
`prediction_internal_log_activity_observed=true` when the INFO log contains
evaluator/predictor/obstacle activity such as `CRUISE_MLP_EVALUATOR`. That is
diagnostic evidence that the Prediction process did internal work. It is not
native `/apollo/prediction` output evidence and must not change
`prediction_mode=missing` into `native_observed`.
For Apollo transition/CyberRT runs, the backend also snapshots selected Apollo
bvar dumps, including `artifacts/apollo_planning.data`, with metadata in
`artifacts/apollo_bvar_dump_snapshot_meta.json`. When that dump contains
`mainboard_planning_apollo_prediction_recv_msgs_nums > 0`, the prediction
analyzer may report `prediction_mode=native_observed` with
`prediction_message_count_source=planning_bvar`. This is stronger than INFO log
activity because it proves Apollo Planning consumed prediction messages, but it
must remain source-labelled and should not be confused with direct
`channel_stats.json` observation.

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
and `bridge.reference_debug_artifact_sample_stride` to sample wide primary
map/reference debug rows while preserving the route/reference contract and topic
stats artifacts. The bridge records sampled-out counts in
`artifacts/cyber_bridge_stats.json`. Sampling those diagnostics does not relax
required channel, localization, HDMap, reference-line, or control artifacts.

High-rate claim-evidence rows may also be sampled with
`bridge.claim_evidence_artifact_sample_stride` when
`cyber_bridge_stats.json.artifact_buffering` or `publish_gap_trace.jsonl` show
artifact writer lag coinciding with localization/chassis publish-loop gaps. This
only reduces diagnostic IO pressure. It does not change Apollo channel-health
thresholds, and a run with localization/chassis max-gap failures remains
diagnostic until the channel reports recover.

Route-only and materialization claim probes must also keep periodic artifact
flushes out of the GT publish loop: use `artifact_flush_interval_s: 0.0`,
`artifact_flush_max_pending_rows: 0`, and a sampled
`claim_evidence_artifact_sample_stride` such as `10`. These settings preserve the
required row-level artifacts for postprocess while preventing diagnostic IO from
becoming the primary localization/chassis cadence blocker.

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

With `--run-dir`, the exporter first tries to infer the container map directory,
base-map filename, and map name from `artifacts/map_contract_guard.json`; pass
`--map-dir` / `--map-name` only when overriding that run-local contract. For
fixed-scene runs, route samples may come from `route.json`, manifest
`metadata.scenario_metadata.route_trace`, `artifacts/scenario_metadata.json`
front-alignment spawn/target metadata, or finally `artifacts/scenario_goal.json`.
The source is recorded per row so operator-generated projection evidence remains
auditable.

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
