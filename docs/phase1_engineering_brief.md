# Phase 1 Engineering Brief

Last reviewed: 2026-06-21

This brief freezes the Phase 1 engineering target for `carla_testbed`. It is a
scope and status document, not a capability announcement. Status labels below
are based only on repository evidence listed in each row; when evidence is
missing, the status is `NOT_YET` or `UNKNOWN`.

## Long-Term Direction

The long-term goal is not an Apollo-only platform. The long-term goal is:

> A CARLA-based multi-algorithm scenario simulation and evaluation platform.

The platform should eventually support:

- Apollo
- Autoware
- self-developed planning/control modules
- end-to-end models
- VLM / agentic drivers
- other open-source autonomous-driving algorithms

Phase 1 does not try to build that entire platform. Phase 1 uses Apollo as the
first reference backend while preserving a self-written
`PlanningControlBackend` / baseline-style backend so the project can exercise
scenario playback, GT inputs, closed-loop control, artifacts, evaluation, and
comparison in one constrained loop.

## Phase 1 Objective

Phase 1 target:

> Use a fixed scenario player to run reproducible closed-loop comparisons
> between `ApolloBackend` and `PlanningControlBackend` in the same CARLA
> scenario, producing unified artifacts, simple evaluation results, and a
> failure reason.

Phase 1 success does not mean Apollo performs well in every scenario. Phase 1
success means:

1. Fixed scenarios can be replayed reproducibly.
2. Ego is controlled by the selected backend.
3. Non-ego actors are controlled by the fixed scene player.
4. `ApolloBackend` can connect as an external AD-stack-style backend.
5. `PlanningControlBackend` can connect as a self-written module or baseline
   backend.
6. The same scenario case can be run with multiple backends.
7. Every run writes a unified artifact set.
8. Backend comparison can write a simple comparison summary.
9. The system can distinguish backend failure from invalid runs.
10. Simple `v-t-gap` curves can explain longitudinal behavior.

## Current Phase 1 Progress Snapshot

Latest reviewed snapshot: `2026-06-21`, after the latest GPT Pro audit of the
Phase 1 accepted-bundle evidence, the ScenarioComparison completeness resolver
fix, the Baguang lane-event hard-gate audit, a fresh Apollo sidecar cut-in
online validation run, a `carla_builtin` safety-sensor attach-order fix
validated with a new online cut-in run, and lateral/reference-line semantics
postprocess refreshes that consume authoritative Apollo control-apply trace
evidence.

Layered progress status:

- Phase 1 overall: `PARTIAL`
- Accepted comparison-surface catalog: `DONE`
- Phase 1 completion gate: `PARTIAL`
- Representative Apollo behavior capability: `PARTIAL / failing samples remain`
- Apollo natural-driving or no-interference capability claim: `NOT_CLAIMED`

Current accepted comparison-surface catalog status using
`tools/phase1_scenario_catalog.py --repo . --evidence-root runs`:

- Total scenarios tracked: `8`
- Accepted `DONE`: `8`
- `PARTIAL`: `0`
- `NOT_YET`: `0`
- `UNKNOWN`: `0`

Pro-audit reconciliation:

- The `8 DONE / 0 PARTIAL` number above is a local repository state computed
  against the full `runs/` tree. It means each current row has a
  `phase1_acceptance_report.json` that points at exact participating run IDs
  and comparison evidence on this machine.
- It is not automatically transferable to an external review package. If the
  submitted package omits generated catalog JSON/MD, comparison directories,
  exact participating run directories, raw `timeseries.*`, actor/phase/control
  traces, `phase1_status.json`, artifact-completeness reports, assist ledgers,
  and a SHA256 manifest, then the package-level audit status is `PARTIAL` even
  when the local repository can recompute `8 DONE`.
- Phase 1 progress must therefore be reported in three layers:
  local accepted comparison-surface catalog = `DONE`; external review package
  completeness = `PARTIAL` until the self-contained pack is present; Phase 1
  overall = `PARTIAL` until representative Apollo online behavior blockers and
  the external-backend runtime path are reduced.

Interpretation:

- The Phase 1 platform skeleton is substantially built: scenario definitions,
  target actor contracts, `v-t-gap`, status classification, ScenarioComparison,
  and Apollo sidecar evidence all exist.
- The latest Pro audit conclusion should be read literally: Phase 1 is not
  complete yet. The accepted-bundle surface is a useful mechanism and current
  catalog milestone, but it is not the same thing as a Phase 1 completion
  certificate. Phase 1 completion still requires representative same-case
  online bundles that bind exact run IDs, complete artifacts, target
  interaction validity, metric validity, assist status, and comparison verdicts
  without mixing best evidence across unrelated runs.
- The latest Pro-review progress reading is: Phase 1 has moved from artifact
  scaffolding to evidence-backed blocker reduction. The accepted
  comparison-surface catalog is auditable, but Phase 1 is not complete until
  representative online runs demonstrate stable external-backend behavior on
  the same scenario cases. In practical terms, the next cycle should optimize
  the current highest-value Apollo behavior blocker, not create more generic
  validation surfaces unless a missing artifact blocks that diagnosis.
- The latest Pro audit found that the earlier `8/8 DONE` statement was still
  too optimistic because some review-pack surfaces were stale or could not be
  independently re-computed. The follow-up fix keeps accepted-bundle rows tied
  to self-contained materialized evidence and makes ScenarioComparison use the
  Phase 1 artifact-completeness profile rather than the stricter natural-driving
  claim/materialization profile.
- The corrected reading is therefore not "Phase 1 is complete." The corrected
  reading is: "the current eight-row accepted comparison-surface catalog is
  self-contained and auditable, while Phase 1 overall remains `PARTIAL` until
  representative Apollo behavior blockers and the external-backend runtime path
  are reduced enough to support stable same-case online comparisons."
- A stricter Baguang lane-event contract audit temporarily demoted
  `cut_in_simple` from accepted `DONE` to `PARTIAL`. The refreshed Apollo run
  starts inside the cut-in activation window. The Apollo sidecar sample
  `runs/phase1_apollo_sidecar_cut_in_bvar_prediction_online_20260620_181700`
  snapshots `artifacts/apollo_planning.data`; `prediction_evidence_report.json`
  reads Apollo Planning bvar counters and reports native Prediction evidence
  with `prediction_message_count_source=planning_bvar`. That removes the earlier
  `prediction_evidence:closed_loop` primary blocker for this sample. A later
  online sample,
  `runs/phase1_apollo_sidecar_cut_in_event_append_online_20260620_195742`,
  verifies that typed transition events no longer overwrite runtime
  `events.jsonl`: the root event log preserves `run_start`, `lane_invasion`,
  `failure`, `run_end`, and typed transition start/end rows. The lane-invasion
  row includes `crossed_lane_marking_types=["Solid"]`, which gives the
  lane-event contract a real marking source rather than missing evidence.
  The lane-event analyzer now uses a yaw-aware vehicle footprint when runtime
  vehicle length and heading error are available. With that corrected geometry,
  the same Apollo sample reports
  `lane_invasion_event_can_be_used_as_hard_gate=true` and
  `possible_real_lane_departure_or_unclassified_lane_event`. The refreshed
  comparison
  `runs/phase1_comparisons/baguang_cut_in_lane_event_contract_vs_builtin_20260620_201342/comparison_summary.json`
  is therefore `partially_evaluable` with reason
  `lane_event_contract_blocks_hard_gate` and target status
  `shared_safety_event_context_issue`: the Apollo sample is a hard-gate-eligible
  lane-departure failure, but the paired `carla_builtin` comparison run still
  has a low-CTE lane-invasion quarantine, so the backend comparison cannot yet
  produce a clean win/loss verdict.
- The paired `carla_builtin` quarantine was traced to safety sensors attaching
  before CARLA's ego-spawn settling tick. The runner now attaches safety sensors
  after the stabilization tick, so spawn-time lane-invasion transients are setup
  provenance rather than run behavior evidence. The validation run
  `runs/phase1_builtin_baguang_cut_in_safety_attach_online_20260620_203218`
  has `lane_invasion_count=0`, `baguang_lane_event_contract_status=pass`,
  `scenario_actor_contract=pass`, and `v_t_gap=pass`.
- The Apollo sidecar cut-in run uses CARLA absolute `sim_time` in
  `timeseries.csv` while its fixed-scene actor trace starts at run-relative
  time zero. `v_t_gap` now reads the fixed-scene runtime
  `start_sim_time_s`, aligns the actor trace to the timeseries timebase, and
  records `time_alignment.status=applied`. This keeps the cut-in longitudinal
  curves tied to the correct target actor frames.
- All eight current P0/P1 rows now have self-contained Phase 1 accepted `DONE`
  bundles.
  Most rows are under
  `runs/phase1_acceptance_*_selfcontained_control_20260620_185256/acceptance/`;
  `cut_in_simple` is refreshed under
  `runs/phase1_acceptance_cut_in_simple_time_aligned_v_t_gap_20260620_204023/acceptance/`
  and is `DONE`.
  Each bundle has `bundle_self_contained=true` and materializes run summaries,
  manifests, `timeseries.*`, `events.jsonl`, `phase1_status.json`,
  `v_t_gap_report.json`, `v_t_gap.csv`, comparison artifacts, and an executable
  control trace surface under the bundle-local `evidence/` directory. Target
  actor rows also materialize scenario actor trace and phase events.
- This completes only the current Phase 1 accepted comparison-surface catalog.
  It does not complete Phase 1 as a whole and does not mean Apollo behavior
  succeeded in the DONE scenarios: several representative Apollo and builtin
  runs are still evaluable failures, including the latest Apollo cut-in sample
  (`failed/lane_invasion`). The active `cycle=inf` milestone has therefore
  shifted from accepted-bundle construction to representative Apollo behavior
  blocker reduction and cleaner external-backend runtime integration.
- The current Phase 1 completion gap is therefore no longer primarily a missing
  catalog-mechanism problem. The active gap is behavior/evidence closure:
  representative Apollo online runs must become stable enough for same-case
  comparison without blocking assists, stale transition metadata, target-phase
  invalidity, or missing route/reference-line lateral semantics evidence.
- For representative failed Phase 1 runs, `phase1_status.json` now surfaces a
  `primary_behavior_blocker`, `behavior_blocker_layer`, and
  `behavior_next_action` distilled from derived evidence such as
  `baguang_lane_event_contract` and `control_health`. These fields are operator
  triage aids only: they explain why an evaluable scenario failed, but they do
  not reclassify invalid runs, do not change ScenarioComparison acceptance, and
  do not create Apollo natural-driving capability evidence.
- Phase 1 postprocess refreshes Apollo control evidence in dependency order:
  `apollo_control_handoff` first, then `control_attribution`, then
  `control_health`. This avoids stale `control_attribution_report.json` files
  built from sparse `timeseries.csv` rows when richer
  `artifacts/control_apply_trace.jsonl` evidence is available.
- `apollo_link_health` now prefers an explicit
  `analysis/assist_ledger/assist_ledger.json` over stale embedded
  `summary.json` / `manifest.json` assist ledgers, and it uses
  `planning_materialization.claim_window_nonempty_ratio` when that claim
  window is explicitly sourced from `after_routing_success` or
  `after_first_nonempty_trajectory`. On the latest representative Apollo
  cut-in run, the no-assist layer therefore reports `active_assists=[]` and
  `planning_nonempty_ratio≈0.987` for the after-routing claim window instead
  of blocking on stale `legacy_followstop` metadata or startup empty
  trajectories. The refreshed link-health primary blocker is now
  `natural_driving_outcome:insufficient_data`; the Phase 1 behavior report
  still records `failed/lane_invasion`; after the lateral-semantics caveat is
  consumed, the current `primary_behavior_blocker` is
  `lane_departure_with_route_simple_lat_sign_convention_candidate`.
  The next behavior loop should inspect lateral/reference-line semantics and
  raw/mapped/applied steer around the lane event, not assist cleanup or
  startup planning warmup.
- `apollo_lateral_semantics` now consumes both
  `artifacts/control_apply_trace.jsonl` and
  `artifacts/control_decode_debug.jsonl`, then time-aligns raw/mapped/applied
  control and Apollo `simple_lat` debug fields into sparse `timeseries.*` rows.
  On the latest representative Apollo cut-in run, the refreshed report shows
  the first high-lateral sample at `sim_time≈10320.679s` with
  `apollo_steer_raw≈0.494`, `bridge_steer_mapped≈0.124`,
  `carla_steer_applied≈0.115`, `cross_track_error≈0.513m`,
  `apollo_simple_lat_lateral_error≈-0.546m`, and
  `heading_error≈-0.128rad`. That removes the earlier false appearance that
  source steer was zero or that Apollo simple_lat evidence was missing during
  the lane event.
  The report is still `warn`, with suspected layer
  `reference_line_semantics` because Planning is non-empty while reference-line
  debug remains missing/zero (`reference_line_count_zero_ratio=1.0` and
  `reference_line_provider_ready_ratio=0.0`). This is attribution evidence, not
  a behavior fix or an Apollo natural-driving claim.
- The same lateral-semantics refresh now records a conservative
  `lateral_sign_alignment` diagnostic. For the first high-lateral sample in
  `runs/phase1_apollo_sidecar_cut_in_event_append_online_20260620_195742`,
  `source_steer_vs_route_lateral_error=same_sign` and
  `applied_steer_vs_route_lateral_error=same_sign`, while
  `route_lateral_error_vs_simple_lat_lateral_error=opposite_sign` and
  `source_steer_vs_simple_lat_lateral_error=opposite_sign`. Across all active
  high-lateral samples, the route-lateral / Apollo simple_lat opposite-sign
  ratio is `1.0`, the source-steer / route-lateral same-sign ratio is `1.0`,
  and the source-steer / Apollo simple_lat lateral same-sign ratio is `0.0`.
  The next diagnostic refinement quantifies whether the opposite signs are also
  matching-magnitude pairs. On the refreshed report, 18 active samples have
  `route_simple_lat_magnitude_alignment.magnitude_agreement_candidate=true`,
  `opposite_sign_ratio=1.0`, and
  `opposite_sign_abs_sum_p95_m≈0.028m`. The preferred interpretation is now a
  route/simple_lat sign-convention candidate: validate the CARLA route
  cross-track convention against Apollo simple_lat semantics before changing
  control mapping, steering scale, smoothing, or controller gains. This is
  still a `warn`-level attribution result, not a behavior fix and not an Apollo
  capability claim. The provenance is explicitly bounded:
  `route_lateral_provenance.evidence_level=hdmap_projection_consistency`,
  because this run lacks `route_x` / `route_y` / `route_heading` samples that
  would let the analyzer recompute signed route CTE from route geometry alone.
  Manual inspection confirms that the run-local `route.json` is a
  `route_stub.v1` artifact with no route points, while
  `route_definition_claim.json` declares sample count without materialized
  route samples. The next validation should materialize `route_x` /
  `route_y` / `route_heading` fields, materialized route samples, or an
  equivalent official projection-to-route pairing before treating the sign
  convention as verified.
- The lateral-semantics analyzer now also consumes row-level
  `artifacts/apollo_hdmap_projection.jsonl` when present. On the same run, 80
  official Apollo HDMap projection rows are available and 13 high-lateral
  samples time-align with the sparse timeseries. The row-level evidence shows
  `route_lateral_vs_projection_lateral.opposite_sign_ratio=1.0` and
  `simple_lat_vs_projection_lateral.same_sign_ratio=1.0` on lane `0_0_2`.
  However, the route/projection magnitude delta p95 is about `0.195m`, so this
  supports the sign-convention suspicion but still does not prove that
  `cross_track_error` and Apollo projection/simple_lat are the same quantity
  with only a sign flip.
  The analyzer now writes
  `analysis/apollo_lateral_semantics/apollo_lateral_projection_pairing.csv`
  when such matched projection samples exist, so review packs can audit the
  row-level pairing instead of trusting only aggregate JSON metrics. The CSV
  includes a centerline point reconstructed from official `projection_l` and
  `lane_heading_at_s` when available; this is projection geometry evidence,
  not materialized scenario route geometry.
  The same pairing now checks Apollo Control simple_lat matched and target
  point coordinates against the local HDMap projection line. On the latest
  cut-in sample, matched/target point lateral offsets are near zero
  (`p95≈1e-6m`) while `current_reference_point` coordinates are not present in
  that trace; the report records
  `point_coverage_status=matched_and_target_available_current_reference_missing`
  rather than leaving this as an implicit `null`. This narrows the suspected
  gap away from raw matched/target point coordinates being off-lane and toward
  the route/simple_lat sign convention plus reference-line debug/export
  semantics. The same report now also records
  `simple_lat_station_vs_projection_s`, comparing official Apollo
  `projection_s` rows with Control debug `current_station`, `matched_point_s`,
  and `target_point_s`. These station deltas are diagnostic because Control may
  use a local/stitching station frame rather than raw lane-s, but they are the
  next narrow evidence needed before changing steering scale, smoothing, or
  actuation mapping. On the same cut-in sample, station coverage is complete,
  `target_s_minus_current_station_abs_p95≈0.043m`, while
  `current_station_minus_projection_s_abs_p95≈25.08m` and
  `target_s_minus_projection_s_abs_p95≈25.12m`; that pattern points toward a
  `local_station_frame_offset_candidate` rather than missing Control station
  debug evidence. This classification only narrows the reference-line /
  simple_lat semantics investigation; it is not a pass/fail gate and does not
  justify changing steering scale, smoothing, or actuation mapping.
  `apollo_link_health` now also echoes this as
  `planning_control_station_bridge` when the Planning reference-line report is
  classified as a debug/export gap, so operators can see the cross-layer
  relation without treating it as reference-line correctness.
- The reference-line contract now also compares Planning first trajectory
  points against official Apollo HDMap projection rows when both are present.
  On the same cut-in sample, 113 time-aligned Planning/projection pairs produce
  `planning_first_point_on_hdmap_projection_line_candidate`,
  `planning_first_point_lane_l_abs_p95≈1.1e-6m`, and
  `planning_first_point_lane_heading_error_p95≈2.3e-7rad` on lane `0_0_2`.
  This narrows the blocker: Planning trajectory first points are locally on the
  official projected lane, while reference-line debug counters remain zero and
  Control simple_lat station appears to use a local/stitching station frame.
  The result is diagnostic-only and does not prove full reference-line
  validity, route equivalence, or behavior success.
- The lateral-semantics report now makes that station-frame evidence explicit
  with `route_station_frame_alignment`. On the same cut-in sample,
  run-local `route_s` is close to Apollo Control `current_station` and
  `target_point_s` (`route_s_current_station_abs_delta_p95≈0.150m`,
  `route_s_target_point_s_abs_delta_p95≈0.140m`), while Control station remains
  offset from official Apollo `projection_s` by about `25.08m`. The current
  classification is
  `route_s_and_simple_lat_share_local_station_frame_candidate`. This means the
  remaining investigation should compare lateral sign conventions and route
  frame definitions, not treat Control station as missing or immediately tune
  steering/control.
- `apollo_link_health` now uses that lateral-semantics warning as the
  representative Apollo cut-in primary blocker when all upstream link layers
  are non-blocking and `natural_driving_report.json` is merely absent. The
  refreshed primary blocker is
  `apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate`;
  `natural_driving_outcome:insufficient_data` remains a secondary blocker and
  still prevents any unassisted natural-driving claim. The reference-line debug
  export gap remains important context, but it is no longer the latest primary
  blocker for this specific cut-in sample.
- Manual artifact inspection of the same run confirms the next evidence gap:
  `apollo_reference_line_debug.jsonl`,
  `stage5_apollo_reference_line_debug.jsonl`, and
  `planning_route_segment_debug.jsonl` each contain `316` rows with
  `reference_line_count=0` throughout, while the final Planning debug row has
  `route_segment_count=1`, lane id `0_0_2`, and non-empty trajectory evidence.
  `bridge_control_decode.jsonl` also contains `796` rows of Apollo Control
  simple_lat target-point / curvature / planning-lateral-consumption debug.
  The next highest-value validation is therefore to close the gap between
  Planning reference-line debug/export evidence and Control simple_lat
  reference semantics, before changing actuation mapping.
- The reference-line contract now records this as a structured diagnostic:
  `reference_debug_diagnostic.classification=planning_reference_line_debug_export_gap`.
  On the same run, that diagnostic reports claim-window non-empty trajectory
  ratio `0.994`, `route_segment_available=true`, `reference_line_count_zero_ratio=1.0`,
  `reference_line_provider_ready_ratio=0.0`, and
  `control_simple_lat_reference_available=true`. This makes the next blocker
  an evidence/export closure problem between Planning reference-line debug and
  Control simple_lat semantics, not a reason to change Apollo source,
  `steer_scale`, or CARLA actuation mapping.
- Existing comparable failures remain useful blocker evidence. They must not be
  rewritten as Apollo natural-driving success; Phase 1 completion requires
  accepted comparison-surface evidence, not backend behavior success.

## Phase 1 Non-Goals

Phase 1 does not include:

- Autoware backend work beyond preserving existing experimental support.
- End-to-end image policy integration.
- VLM driver integration.
- Full sensor-level full-stack benchmark.
- Random traffic-flow mining as a core benchmark.
- OpenSCENARIO / ScenarioRunner standardization.
- Web UI.
- Cloud batch scheduling.
- Large-scale scenario library.
- Large-scale black-box parameter search.
- Apollo source-code modification.
- Default physical mapping enablement.
- Automatic calibration-profile promotion; any future promotion would require
  `calibration_report.json` and no-regression gates.
- Treating short-window positives as completed natural-driving evidence.

Those can be future extensions. They are not Phase 1 completion criteria.

## Architecture Boundary

### Scenario Playback Layer

Phase 1 uses the existing `fixed_scene_player` / `scenario_player` surface as
the core fixed scenario playback layer.

It owns:

- fixed scenario actor spawning
- non-ego actor behavior playback
- target actor declaration
- phase events
- scenario actor trace
- fixed scene resolved artifacts

It does not own:

- launching Apollo
- controlling ego
- final evaluation
- judging whether Apollo behavior is good
- A/B comparison
- random traffic-flow behavior

Current repository evidence:

- `carla_testbed/scenario_player/`
- `docs/scenario_player.md`
- `configs/scenario_templates/*.yaml`
- `configs/scenarios/baguang/*.yaml`
- `configs/scenarios/town01/*.yaml`
- `tests/test_fixed_scene_compiler.py`
- `tests/test_fixed_scene_player.py`
- `tests/test_fixed_scene_contract.py`

### Backend Layer

Phase 1 focuses on two backend categories:

- `ApolloBackend`
  - External AD-stack / full-stack-like backend.
  - Consumes GT-assisted inputs through adapter/bridge code.
  - Can output Apollo control, trajectory, and debug evidence when available.
- `PlanningControlBackend`
  - Self-written planning/control module or baseline backend.
  - May directly consume `SceneTruth` / scenario truth.
  - Outputs executable control and optionally trajectory/debug.

`PlanningControlBackend` is a Phase 1 target category, not necessarily the
current runtime class name. Current implementation may still use
`carla_builtin` / `simple_acc_route_follower`; do not rename runtime modules
just for documentation consistency.

Phase 1 does not require identical internal inputs for all backends.

> Phase 1 performs scenario-level closed-loop comparison, not identical-input
> algorithm benchmarking.

中文解释：

> 第一阶段进行的是场景级闭环表现对比，不强制所有后端使用完全相同的数据结构或信息粒度。不同后端可以按自身接口消费输入，但必须在 manifest 中声明输入契约、转换路径和可用字段。

The same comparison must still keep these variables aligned:

- same `ScenarioCase`
- same CARLA map
- same fixed scene
- same ego initial condition
- same evaluation artifact surface
- manifest-declared backend input contract
- manifest-declared adapter path
- manifest-declared available truth fields

Current repository evidence:

- `carla_testbed/backends/base.py`
- `carla_testbed/backends/apollo_cyberrt.py`
- `carla_testbed/backends/carla_builtin.py`
- `carla_testbed/backends/registry.py`
- `configs/platforms/apollo_cyberrt.yaml`
- `configs/platforms/carla_builtin.yaml`
- `configs/algorithms/apollo/apollo10_carla_gt.yaml`
- `configs/algorithms/builtin/simple_acc_route_follower.yaml`
- `tests/test_platform_backends.py`
- `tests/test_platform_run_plan.py`

### Output Boundary

Phase 1 unifies on executable control:

- throttle
- brake
- steer

Optional backend-specific records:

- planned trajectory
- raw control
- mapped control
- applied control
- high-level decision
- backend debug

The common boundary is closed-loop execution result plus artifacts, not a
single internal interface. Backends with trajectory/debug evidence should record
it, but trajectory is not a mandatory output for every backend.

Current repository evidence:

- `carla_testbed/contracts/`
- `carla_testbed/control/simple_acc_route_follower.py`
- `tools/run_builtin_ego_fixed_scene.py`
- `carla_testbed/scenario_player/builtin_ego_runner.py`

## Run And Comparison Units

Minimum run unit:

> `ScenarioRun = ScenarioCase + Backend + RunConfig`

Minimum comparison unit:

> `ScenarioComparison = Same ScenarioCase + Multiple Backend Runs`

Examples:

- `follow_stop_static_300m x ApolloBackend`
- `follow_stop_static_300m x PlanningControlBackend`
- `follow_stop_static_300m: ApolloBackend vs PlanningControlBackend`

Recommended artifact layout:

```text
runs/
  <timestamp>_<scenario_case>_<backend>/
    manifest.json
    resolved_config.yaml
    summary.json
    events.jsonl
    timeseries.*
    fixed_scene_resolved.json
    scenario_actor_trace.jsonl
    scenario_phase_events.jsonl
    plots/

comparisons/
  <scenario_case>_<backend_a>_vs_<backend_b>/
    comparison_manifest.json
    comparison_summary.json
    comparison_curves/
```

Current repository state differs slightly. Standard run artifacts are documented
in `docs/run_artifacts.md`, and `RunArtifactStore` writes
`config.resolved.yaml` rather than `resolved_config.yaml`. Existing fixed-scene
runtime artifacts live under `artifacts/`. Do not rename these paths in Phase 1;
use the brief as the convergence target.

Scenario comparison validity:

- A comparison can judge backend behavior differences only when all
  participating runs are evaluable.
- If any participating run is `invalid`, the comparison must be marked
  `invalid` or `partially_evaluable`.
- An invalid run must not count as a backend loss.

Phase 1 accepted scenario status:

- A catalog row may show CARLA online, Apollo online, `v-t-gap`, and
  ScenarioComparison evidence separately, but it is `DONE` only when a
  `phase1_acceptance_report.json` binds one exact Apollo run, one exact
  PlanningControl run, and one exact accepted comparison for the same
  ScenarioCase.
- Cross-run best-of evidence is useful for navigation and triage, but it cannot
  by itself mark a scenario complete.
- The accepted bundle must record participating run ids/dirs, comparison id,
  artifact hashes, validity gates, and blocking reasons. A missing or `PARTIAL`
  accepted bundle keeps the scenario catalog row `PARTIAL`.

## Phase 1 Scenario Scope

### P0 Scenarios

1. `follow_stop_static`
   - Lead vehicle is stationary with brake hold.
   - Ego approaches and stops.
   - The historical follow-stop target should be interpreted this way for Phase
     1 static follow-stop, not as lead cruise then braking.
2. `lead_decel_accel`
   - Lead vehicle first drives, then decelerates, then accelerates again.
   - Used to inspect ego following, deceleration, and recovery.
3. `cut_in_simple`
   - Adjacent-lane target vehicle cuts in ahead of ego.
   - After cut-in, it becomes the target vehicle.
4. `lane_keep_straight`
   - Straight-lane keeping.
   - Basic lateral gate.
5. `lane_keep_curve`
   - Curve lane keeping.
   - Basic lateral gate.

### P1 Scenarios

6. `cut_out_simple`
   - Lead vehicle cuts out.
   - Inspects target transition and speed recovery.
7. `junction_turn_no_signal`
   - Unsignalized junction / turn passing.
8. `lead_hard_brake`
   - Lead vehicle hard-brakes.
   - Inspects safety boundary.

## Longitudinal Evaluation

Phase 1 should stay simple: status plus `v-t-gap` curves.

Core curves:

- `ego_v_t`
- `target_v_t`
- `gap_t`
- `relative_speed_t`
- optional `ego_accel_t`
- optional `target_accel_t`
- optional `throttle_t`
- optional `brake_t`
- optional `steer_t`

Core gap definition:

> `gap_t` is bumper-to-bumper distance from ego front bumper to target rear
> bumper.

The target vehicle is declared by the scenario player / scenario case in Phase
1. The platform should not use per-frame nearest-front-vehicle guessing as the
main target selection logic. Automatic target selection can be a future fallback.

Route-only cases such as lane keeping, curve diagnostics, and junction turns do
not require a target actor. Their `v-t-gap` report should be
`status=not_applicable` with a `target_actor_contract.status=not_required`.
That is valid Phase 1 evidence for "no target-gap metric applies"; it is not a
missing target actor failure and it is not a longitudinal-following success.

## Target Actor Contract

Phase 1 target actor resolution is intentionally simple:

1. Prefer an explicit target actor declared by `scenario_case`.
2. If no explicit field exists yet, use the fixed-scene role marker.
3. If the scenario class declares no target actor is required, write
   `target_actor_contract.status=not_required`.
4. If target evidence is required and still missing, the run is not evaluable
   for `v-t-gap` comparison.

P0 fallback rules:

- `follow_stop_static`: target is `lead_vehicle`.
- `lead_decel_accel`: target is `lead_vehicle`.
- `cut_in_simple`: target is `cutin_vehicle`; the gap is active only after the
  scenario phase says the target has entered the ego lane.
- `cut_out_simple`: may require target transition later; this is not required
  for Phase 1 P0.

Nearest-front-vehicle automatic selection is a future fallback only. It must
not be the Phase 1 primary target selection logic.

Simple lateral gates stay available:

- `lateral_error_t`
- `heading_error_t`
- optional `route_progress`
- `off_route`
- `lane_invasion`

Do not introduce a complex comfort score, full TTC taxonomy, or all-purpose
driving score in Phase 1.

## Run Status And Failure Reason

Phase 1 run status:

- `success`
- `degraded`
- `failed`
- `invalid`

Definitions:

- `failed`: the backend failed in an otherwise valid scenario.
- `invalid`: the run itself is not evaluable and must not count as a backend
  loss.

Suggested invalid reasons:

- `setup_failed`
- `missing_target_actor`
- `missing_required_artifact`
- `backend_not_ready`
- `no_timeseries`
- `fixed_scene_failed`
- `config_invalid`

Suggested failed reasons:

- `collision`
- `no_control`
- `planning_control_handoff_missing`
- `stuck`
- `overshoot_target`
- `unsafe_gap`
- `timeout`
- `off_route`
- `lane_invasion`

Suggested degraded reasons:

- `late_response`
- `large_final_gap`
- `small_final_gap`
- `unstable_control`
- `degraded_gap_method`

The important boundary is: scenario-player, artifact, config, or bridge setup
problems are `invalid`, not backend performance failures.

## Artifact Expectations

Each `ScenarioRun` should have:

- `manifest.json`
- `resolved_config.yaml` or `config.resolved.yaml`
- `summary.json`
- `events.jsonl`
- `timeseries.*`
- `fixed_scene_resolved.json` when fixed scene playback is used
- `fixed_scene_runtime_state.json` when available
- `scenario_actor_trace.jsonl` when fixed scene playback is used
- `scenario_phase_events.jsonl` when fixed scene playback is used
- ego/control trace when available
- `plots/` for future `v-t-gap` curves

The canonical Phase 1 timeseries surface is `timeseries.*`. Current accepted
formats are `timeseries.csv` and `timeseries.jsonl`. Analysis tools must declare
which format they consume, and this phase must not rename existing artifact
paths just to normalize naming.

Each `ScenarioComparison` should have:

- `comparison_manifest.json`
- `comparison_summary.json`
- `comparison_curves/`
- links to participating run artifacts

Repository evidence:

- `docs/run_artifacts.md`
- `carla_testbed/record/artifact_store.py`
- `tests/unit/recording/test_artifact_store.py`
- `carla_testbed/scenario_player/builtin_ego_runner.py`
- `tools/analyze_baguang_stack_comparison.py`
- `tests/test_baguang_stack_comparison.py`

## Capability Status Table

Status definitions:

- `DONE`: implemented and backed by code, tests, artifacts, or commands. For a
  Phase 1 scenario row, `DONE` additionally requires an accepted
  `phase1_acceptance_report.json`; readiness, comparison, or per-backend
  evidence alone is not enough.
- `PARTIAL`: there is an implementation surface, but it cannot yet support the
  Phase 1 claim by itself.
- `NOT_YET`: Phase 1 needs it, and no current implementation evidence was
  found.
- `UNKNOWN`: more repository review or a local run is needed.

| Capability | Phase 1 role | Status | Evidence path | Gap | Suggested next PR |
| ---------- | ------------ | ------ | ------------- | --- | ----------------- |
| fixed_scene_player offline compiler/storyboard layer | Compile fixed scene templates to deterministic storyboards | DONE | `carla_testbed/scenario_player/compiler.py`; `carla_testbed/scenario_player/schema.py`; `configs/scenario_templates/*.yaml`; `tests/test_fixed_scene_compiler.py` | This proves offline storyboard construction, not online CARLA playback quality. | Keep compiler stable; add catalog summary before extending scenarios. |
| fixed_scene_player CARLA online playback validation | Validate non-ego actor playback in CARLA | PARTIAL | `carla_testbed/scenario_player/carla_runtime.py`; `docs/scenario_player.md`; `tests/test_fixed_scene_carla_runtime.py`; `tools/run_fixed_scene_carla_smoke.py`; `tools/run_builtin_ego_fixed_scene.py` | Runtime support exists and latest local evidence includes CARLA builtin playback for all current P0/P1 catalog rows, but this remains host/run dependent and is not Apollo evidence. | Keep catalog evidence explicit and add Apollo fixed-scene compatibility before marking scenarios DONE. |
| scenario case catalog | Enumerate reusable ScenarioCases | DONE | `carla_testbed/analysis/phase1_scenario_catalog.py`; `tools/phase1_scenario_catalog.py`; `carla_testbed/analysis/phase1_acceptance.py`; `tools/phase1_acceptance.py`; `artifacts/phase1_scenario_catalog_current/phase1_scenario_catalog.json`; `tests/test_phase1_scenario_catalog.py`; `tests/test_phase1_acceptance.py` | Catalog now reports `8 DONE / 0 PARTIAL`. All current P0/P1 rows have self-contained accepted bundles with run summaries, manifests, `timeseries.*`, `events.jsonl`, phase/status reports, v-t-gap evidence, comparison artifacts, and executable control trace surface. `cut_in_simple` is accepted after the Apollo event was made hard-gate eligible by yaw-aware footprint analysis and the paired builtin run was refreshed after safety sensors moved behind the spawn-settling tick. This is Phase 1 comparison-surface evidence, not Apollo natural-driving success. | Use the accepted catalog to prioritize representative Apollo behavior blocker reduction and keep new scenario rows behind the same acceptance bundle gate. |
| follow_stop_static | P0 static lead stop | DONE | `configs/scenarios/baguang/follow_stop_static_300m.yaml`; `configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml`; `configs/scenario_templates/static_lead_stop.yaml`; `runs/phase1_acceptance_follow_stop_static_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py`; `tests/test_phase1_scenario_catalog.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py` | Self-contained accepted comparison bundle exists and includes target actor traces, phase events, v-t-gap, comparison, acceptance, and control trace surfaces. The representative Apollo run is still an evaluable behavior failure, so this is not Apollo follow-stop success. | Use accepted evidence to prioritize behavior blocker reduction rather than rebuilding the comparison surface. |
| lead_decel_accel | P0 lead speed-change following | DONE | `configs/scenario_templates/lead_vehicle_accel_decel.yaml`; `configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml`; `configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml`; `runs/phase1_acceptance_lead_decel_accel_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py`; `tests/test_v_t_gap_extractor.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py` | Self-contained accepted comparison bundle exists with target actor, phase, v-t-gap, and control trace evidence. Participating runs remain evaluable failures, so this is comparison-surface completion only. | Use the bundle to compare failure timing and reduce representative Apollo/Baguang lane-invasion blockers. |
| cut_in_simple | P0 target enters ego lane | DONE | `configs/scenario_templates/cut_in.yaml`; `configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml`; `configs/scenarios/town01/cut_in_097.yaml`; `runs/phase1_apollo_sidecar_cut_in_bvar_prediction_online_20260620_181700/artifacts/apollo_planning.data`; `runs/phase1_apollo_sidecar_cut_in_bvar_prediction_online_20260620_181700/analysis/prediction_evidence/prediction_evidence_report.json`; `runs/phase1_apollo_sidecar_cut_in_event_append_online_20260620_195742/events.jsonl`; `runs/phase1_apollo_sidecar_cut_in_event_append_online_20260620_195742/analysis/baguang_lane_event_contract/baguang_lane_event_contract_report.json`; `runs/phase1_builtin_baguang_cut_in_safety_attach_online_20260620_203218/summary.json`; `runs/phase1_comparisons/baguang_cut_in_time_aligned_v_t_gap_20260620_204023/comparison_summary.json`; `runs/phase1_acceptance_cut_in_simple_time_aligned_v_t_gap_20260620_204023/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_player.py::test_baguang_cut_in_starts_at_configured_activation_gap`; `tests/test_apollo_prediction_evidence.py::test_planning_bvar_prediction_count_counts_as_native_observed_with_source`; `tests/test_scenario_comparison.py::test_lane_event_contract_blocks_lane_invasion_backend_loss`; `tests/test_baguang_lane_event_contract.py::test_downstream_lane_invasion_before_footprint_crossing_is_quarantined`; `tests/test_baguang_lane_event_contract.py::test_yaw_aware_vehicle_footprint_prevents_false_quarantine`; `tests/test_v_t_gap_extractor.py::test_v_t_gap_aligns_relative_actor_trace_to_absolute_apollo_timeseries`; `tests/unit/test_apollo_compat_runtime.py::test_transition_events_append_without_erasing_backend_events`; `tests/test_builtin_ego_runner_safety_integration.py`; `tests/test_phase1_acceptance.py` | The newest correctly configured Apollo cut-in sample preserves root `events.jsonl` runtime events and includes `crossed_lane_marking_types=["Solid"]`; scenario actor contract passes and `v_t_gap` is available. After yaw-aware vehicle footprint correction, the Apollo lane event is hard-gate eligible and classified as `possible_real_lane_departure_or_unclassified_lane_event`. The refreshed builtin run attaches safety sensors after ego spawn settling, has `lane_invasion_count=0`, and clears the previous low-CTE quarantine. The final comparison is `comparable/apollo_vs_planning_control_evaluable`; its Apollo `v_t_gap` curve uses fixed-scene start-time alignment and the acceptance bundle is self-contained `DONE`. This is accepted Phase 1 comparison-surface evidence; Apollo's representative run still failed with lane invasion. | Use this accepted bundle to reduce Apollo cut-in behavior blockers; do not reinterpret it as Apollo cut-in success. |
| lane_keep_straight | P0 lateral baseline | DONE | `configs/scenarios/town01/lane_keep_097.yaml`; `runs/phase1_acceptance_lane_keep_straight_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_scenario_catalog.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Route-only Phase 1 comparison-surface evidence is self-contained and accepted, including executable control trace surfaces. Apollo's participating run remains an evaluable failure, not natural-driving capability evidence. | Use this accepted route-only row as the template for future review packs. |
| lane_keep_curve | P0 lateral curve baseline | DONE | `configs/scenarios/town01/curve217_diagnostic.yaml`; `runs/phase1_acceptance_lane_keep_curve_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_status_classifier.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Route-only curve comparison-surface evidence is self-contained and accepted. Apollo behavior remains diagnosable with route/reference-line style blockers; do not tune control from this result alone. | Use this surface for route/reference-line blocker reduction. |
| cut_out_simple | P1 target exits ego lane | DONE | `configs/scenario_templates/cut_out.yaml`; `configs/scenarios/baguang/cut_out_35kph_right_to_left_25m.yaml`; `configs/scenarios/town01/cut_out_097.yaml`; `runs/phase1_acceptance_cut_out_simple_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py::test_baguang_cut_out_compiles_lane_change_phase`; `tests/test_phase1_acceptance.py`; `tests/test_apollo_fixed_scene_launch_plan.py::test_apollo_baguang_cut_out_launch_plan_uses_sidecar_runtime_command` | Self-contained accepted comparison bundle exists with lane-change actor/phase evidence and control trace surfaces. Both participating runs remain evaluable `lane_invasion` failures, so this is not cut-out behavior success. | Keep lane-change playback evidence separate from ego autonomy success and use the bundle for blocker triage. |
| junction_turn_no_signal | P1 junction/turn case | DONE | `configs/scenarios/town01/junction_031.yaml`; `runs/phase1_acceptance_junction_turn_no_signal_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_scenario_binding.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Route-only junction comparison-surface evidence is self-contained and accepted. Apollo remains evaluable with runtime/route-establishment blockers; this no-signal row is not traffic-light or natural-driving evidence. | Use the accepted bundle to inspect route-establishment and junction behavior blockers. |
| lead_hard_brake | P1 hard-brake following | DONE | `configs/scenarios/baguang/lead_hard_brake_70_to_0_20m.yaml`; `runs/phase1_acceptance_lead_hard_brake_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py::test_baguang_lead_hard_brake_profile_matches_70_to_stop`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py` | Self-contained accepted comparison bundle exists with target actor, phase, v-t-gap, and control trace evidence. Both participating runs remain evaluable `lane_invasion` failures and Apollo link-health still reports runtime/module blockers. | Preserve the accepted bundle and shift effort to representative Apollo blocker reduction. |
| ApolloBackend | Reference external AD-stack backend | PARTIAL | `carla_testbed/backends/apollo_cyberrt.py`; `configs/platforms/apollo_cyberrt.yaml`; `configs/algorithms/apollo/apollo10_carla_gt.yaml`; `tools/run_phase1_scenario.py`; `runs/phase1_apollo_followstop_obstacle_type_metadata_20260618_012159/analysis/phase1_status/phase1_status.json`; `tests/test_platform_backends.py`; `tests/test_apollo_fixed_scene_launch_plan.py`; `tests/test_phase1_status_classifier.py` | Facade and bridge evidence exist, fixed-scene preflight can write structured `backend_not_ready` artifacts, and one legacy Apollo follow-stop compatibility run is now evaluable as `failed/unsafe_gap` rather than invalid target/setup evidence. Actual fixed-scene Apollo runtime dispatch is still not migrated behind the facade, and this is not Apollo natural-driving success. | Migrate the compatibility path behind the ApolloBackend facade and compare the same ScenarioCase against `PlanningControlBackend` without weakening invalid-run semantics. |
| PlanningControlBackend | Self-written planning/control baseline backend | PARTIAL | `carla_testbed/backends/carla_builtin.py`; `configs/platforms/carla_builtin.yaml`; `configs/algorithms/builtin/simple_acc_route_follower.yaml`; `carla_testbed/control/simple_acc_route_follower.py`; `carla_testbed/scenario_player/builtin_ego_runner.py`; `runs/phase1_builtin_follow_stop_097_global_route_20260618_030049/analysis/phase1_status/phase1_status.json`; `runs/phase1_builtin_lead_accel_decel_097_goal_cycle_20260618_031551/analysis/phase1_status/phase1_status.json`; `runs/phase1_builtin_baguang_lead_decel_safety_surface_20260619_210418/analysis/phase1_status/phase1_status.json`; `tests/test_builtin_ego_controller.py`; `tests/test_manifest_input_contract.py` | Current runtime name remains `carla_builtin`; manifests identify it with `backend_type=planning_control_backend`, `input_contract=scene_truth_direct`, and diagnostic-only claim boundaries. It supports fixed-scene and route-only Phase 1 diagnostic runs. The Town01 `lead_decel_accel` builtin run remains evaluable success, while the refreshed Baguang lead-decel safety-surface run is evaluable but failed with near-start `lane_invasion`; both are diagnostic baseline evidence, not Apollo/Autoware capability, and any Apollo/Autoware comparison must use the same acceptance gate. | Use these manifest fields in comparison reports; do not rename runtime modules for documentation consistency, and do not count builtin success or failure as Apollo/Autoware capability. |
| ScenarioRun artifact structure | Per-run artifact envelope | PARTIAL | `docs/run_artifacts.md`; `carla_testbed/record/artifact_store.py`; `tests/unit/recording/test_artifact_store.py`; `carla_testbed/scenario_player/builtin_ego_runner.py` | Existing paths differ from the recommended Phase 1 layout; some online tools still write richer legacy artifacts. | Add a non-invasive artifact completeness checker for Phase 1 ScenarioRun. |
| ScenarioComparison artifact structure | Backend comparison envelope | PARTIAL | `carla_testbed/analysis/scenario_comparison.py`; `tools/compare_scenario_runs.py`; `runs/phase1_comparisons/lane_keep_real_apollo_vs_builtin_safety_surface_20260619_164004/comparison_summary.json`; `runs/phase1_comparisons/follow_stop_097_apollo_vs_builtin_20260618_030308/comparison_summary.json`; `runs/phase1_comparisons/baguang_lead_decel_apollo_overlay_vs_builtin_safety_surface_20260619_210445/comparison_summary.json`; `runs/phase1_comparisons/baguang_lead_hard_brake_apollo_overlay_vs_builtin_safety_surface_20260620_004757/comparison_summary.json`; `runs/phase1_comparisons/baguang_cut_in_apollo_overlay_vs_builtin_safety_surface_20260620_005828/comparison_summary.json`; `runs/phase1_comparisons/baguang_cut_out_apollo_overlay_vs_builtin_safety_surface_20260620_010954/comparison_summary.json`; `tests/test_scenario_comparison.py`; Baguang-specific wrapper in `carla_testbed/analysis/baguang_stack_comparison.py` | Generic comparison exists, preserves invalid-run boundaries, records `comparison_target_status` / `backend_coverage`, checks safety-event evidence-surface consistency for `lane_invasion` and `collision`, and now has comparable evidence for all current P0/P1 rows. It is still PARTIAL as an artifact envelope because schemas and plotting surfaces remain minimal. | Keep extending report detail without weakening invalid-run or evidence-surface semantics. |
| v-t-gap extraction | Core longitudinal behavior curves | PARTIAL | `carla_testbed/analysis/v_t_gap.py`; `tools/extract_v_t_gap.py`; `runs/phase1_online_builtin_lane_keep_20260617_135010/analysis/v_t_gap/v_t_gap_report.json`; `runs/phase1_builtin_lead_accel_decel_097_goal_cycle_20260618_031551/analysis/v_t_gap/v_t_gap_report.json`; `runs/phase1_apollo_sidecar_cut_in_event_append_online_20260620_195742/analysis/v_t_gap/v_t_gap_report.json`; `tests/test_v_t_gap_extractor.py` | Extractor supports `timeseries.csv/jsonl` plus actor traces, filters cut-in target activation, emits `not_applicable` for route-only cases, aligns relative fixed-scene actor traces to absolute Apollo sidecar timeseries via `fixed_scene_runtime_hook.start_sim_time_s`, uses same-lane `route_s_bumper_gap` when route/lane evidence is comparable, rejects route-s rows that conflict with the actor-trace forward anchor, and uses `trajectory_progress_bumper_gap` for fixed-scene lane/road transitions or route-s anchor conflicts. Lane-keep comparison uses the route-only boundary; target-actor scenarios still need Apollo evaluable runs and HDMap/Frenet evidence for natural-driving claims. | Add evaluable ApolloBackend runs for fixed-scene target scenarios and keep trajectory-progress gap clearly separated from Apollo HDMap/Frenet claim evidence. |
| target actor declaration | Bind scenario target vehicle | PARTIAL | `configs/scenarios/*` role blocks; `carla_testbed/scenario_player/target_actor.py`; `carla_testbed/scenario_player/compiler.py`; `tests/test_target_actor_resolver.py` | Resolver exists and writes `target_actor_contract`, but target transition semantics still need online validation for P1 cases. | Use the contract in v-t-gap and comparison reports. |
| bumper-to-bumper gap calculation | Define `gap_t` consistently | PARTIAL | `carla_testbed/analysis/gap.py`; `tests/test_bumper_gap.py` | Utility exists, but many current online traces lack actor length/width fields and may use degraded fallback. | Add dimensions to runtime traces where available. |
| run status classification | Separate success/degraded/failed/invalid | PARTIAL | `carla_testbed/analysis/phase1_status.py`; `tools/classify_phase1_run.py`; `carla_testbed/analysis/phase1_postprocess.py`; `tests/test_phase1_status_classifier.py`; `tests/test_phase1_run_postprocess.py` | Classifier exists and Phase 1 postprocess refreshes run-local `summary.json` echo fields after regenerating reports. It now treats fixed-scene phase triggers not reached by ego behavior as evaluable `failed/scenario_phase_trigger_not_reached`, while setup/spawn/actor-control failures remain invalid. Legacy online paths do not all invoke it automatically. | Keep routing legacy online paths through postprocess and avoid reading stale summary echoes as authoritative status. |
| failure_reason classification | Separate backend failure from invalid run | PARTIAL | Existing `summary.fail_reason` usage; `docs/run_artifacts.md`; `carla_testbed/analysis/phase1_status.py`; analysis modules under `carla_testbed/analysis/` | Phase 1 now distinguishes setup invalid from behavior failure for the fixed-scene trigger-not-reached case, but the full failure reason enum is still not frozen across all legacy analyzers. | Add failure reason enum docs plus a small classifier over summary/artifact completeness. |
| manifest input contract | Record backend input contract and truth fields | PARTIAL | `StackContract` in `carla_testbed/backends/base.py`; `configs/algorithms/apollo/apollo10_carla_gt.yaml`; `configs/algorithms/builtin/simple_acc_route_follower.yaml`; `RunArtifactStore.write_manifest()`; `carla_testbed/scenario_player/manifest_contract.py`; `tests/test_manifest_input_contract.py` | Backend contracts now expose `backend_name`, `backend_type`, `input_contract`, runtime start flags, truth fields, `fixed_scene_case`, `target_actor_contract`, and `artifact_contract_version` for new Phase 1 scaffold/builtin paths. Legacy online tools may still write older manifest shapes. | Keep accepting old manifests, but require these fields for new Phase 1 run evidence. |
| executable control trace | Prove backend-produced throttle/brake/steer | PARTIAL | `artifacts/ego_control_trace.jsonl` in `carla_testbed/scenario_player/builtin_ego_runner.py`; Apollo control traces in `tools/apollo10_cyber_bridge/`; `tests/test_apollo_control_handoff.py` | Trace schemas differ between builtin and Apollo paths. | Add a thin normalization report for executable control traces. |
| summary artifact writer | Write a final run summary artifact | DONE | `docs/run_artifacts.md`; `RunArtifactStore.summary_path`; `tests/unit/recording/test_artifact_store.py`; `builtin_ego_runner.py` | Writer exists, but this does not prove Phase 1 semantics. | Keep writer stable. |
| Phase 1 summary semantics | Normalize status/failure semantics for Phase 1 | PARTIAL | Existing `summary.json` writers; `docs/run_artifacts.md`; `carla_testbed/analysis/phase1_status.py`; `carla_testbed/analysis/phase1_postprocess.py` | The classifier is centralized for Phase 1 postprocess, and summary echo fields are refreshed after recomputation. Older summaries may still carry legacy status fields and must be checked against `analysis/phase1_status/phase1_status.json`. | Integrate postprocess into remaining online paths and keep `phase1_status.json` authoritative for Phase 1 evaluation. |
| timeseries.* | Per-frame metrics surface | PARTIAL | `docs/run_artifacts.md`; `builtin_ego_runner.py`; many analyzers read `timeseries.csv`; `RunArtifactStore.timeseries_jsonl_path` | `timeseries.csv` and `timeseries.jsonl` both exist, but target actor and bumper gap fields are not unified. | Add Phase 1 minimal timeseries field contract and require analyzers to declare consumed format. |
| events.jsonl writer | Write discrete run events | DONE | `RunArtifactStore.open_events()`; `tests/unit/recording/test_artifact_store.py`; `docs/run_artifacts.md` | Writer exists, but this does not define Phase 1 event names. | Keep writer stable. |
| Phase 1 event vocabulary | Define target activation and invalid-run events | PARTIAL | Existing `events.jsonl` usage; `docs/run_artifacts.md`; scenario phase events in `carla_testbed/scenario_player/trace.py` | Phase 1 event names for target activation and invalid-run reasons are not frozen. | Add optional Phase 1 event vocabulary. |
| comparison report | Compare multiple backend runs | PARTIAL | `carla_testbed/analysis/scenario_comparison.py`; `tools/compare_scenario_runs.py`; `runs/phase1_comparisons/lane_keep_real_apollo_20260617_140000/comparison_summary.json`; `runs/phase1_comparisons/follow_stop_097_apollo_vs_builtin_20260618_030308/comparison_summary.json`; `runs/phase1_comparisons/baguang_lead_decel_apollo_overlay_vs_builtin_safety_surface_20260619_210445/comparison_summary.json`; `runs/phase1_comparisons/baguang_lead_hard_brake_apollo_overlay_vs_builtin_safety_surface_20260620_004757/comparison_summary.json`; `runs/phase1_comparisons/baguang_cut_in_apollo_overlay_vs_builtin_safety_surface_20260620_005828/comparison_summary.json`; `runs/phase1_comparisons/baguang_cut_out_apollo_overlay_vs_builtin_safety_surface_20260620_010954/comparison_summary.json`; `tests/test_scenario_comparison.py`; `tools/analyze_baguang_stack_comparison.py`; `tests/test_baguang_stack_comparison.py` | Generic report exists and now covers route-only Apollo-vs-builtin comparisons plus fixed-scene follow-stop, lead-decel, hard-brake, cut-in, and cut-out target comparisons. It remains PARTIAL because the report is still intentionally simple and should not be treated as a complete benchmark paper artifact. | Use the comparable failures to prioritize blocker reduction; do not convert comparison completeness into behavior success. |
| docs overclaim guard tests | Prevent broad overclaiming in tracked docs | DONE | `tests/test_docs_no_overclaim.py`; `tests/test_docs_claim_artifacts.py`; `docs/README.md` | Guard tests exist, but they do not by themselves make Phase 1 docs complete. | Keep Phase 1 brief under docs guard. |
| Phase 1 docs claim consistency | Keep Phase 1 language aligned with actual evidence | PARTIAL | `docs/phase1_engineering_brief.md`; docs guard tests | This brief is a first scope freeze; future Phase 1 docs still need to cite exact artifacts when claims mature. | Review future Phase 1 docs against this brief and docs guard. |
| tests / fixtures | CI-safe verification | PARTIAL | `tests/test_fixed_scene_*`; `tests/test_platform_*`; `tests/test_builtin_ego_controller.py`; `tests/test_baguang_stack_comparison.py` | Missing tests for generic `ScenarioRun`, generic `ScenarioComparison`, and `v-t-gap` extraction. | Add focused tests for Phase 1 status and v-t-gap extraction. |

Latest Apollo follow-stop blocker refinement, `2026-06-18`:

- Refreshed Apollo link-health plus a 300m/no-beyond-front online probe refine
  the Baguang shifted-start overlay blocker from broad long-goal / claim-route
  compatibility to `apollo_routing_goal_projection_disabled_by_config`, with
  `apollo_routing_goal_projection_not_accepted` and
  `apollo_routing_goal_snap_distance_high` as supporting blockers. The probe run is
  `runs/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_goal300_probe_20260618_155124`.
  It still failed with `LANE_INVASION_HARD`, so this is not behavior success.
  It means the next highest-value validation is claim-safe Baguang Apollo goal
  snap / projected endpoint configuration, before control tuning or runtime
  smoothing.
- A follow-up probe with `snap_goal_to_lane=true` is
  `runs/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_goal_snap_probe_20260618_182601`.
  It removed the `disabled_by_config` blocker, but `apollo_route_contract`
  still fails with `apollo_routing_goal_snap_distance_high`; Apollo routing
  length becomes about `305.1m`, and reference-line evidence remains
  insufficient. The run completed 700 ticks with no collision or lane invasion,
  but the harness summary reports `NO_SENSOR_DATA`, so Phase 1 status is
  `invalid/missing_required_artifact`, not backend success.

Latest Apollo follow-stop cadence refinement, `2026-06-19`:

- Refreshed link-health for
  `runs/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_compat_20260619_101626`
  reports `primary_blocker=control_mapping_apply:apollo_raw_command_oscillation`.
  This is not a control-mapping fix request. The primary detail now exposes
  `gt_state_sampling_cadence`: CARLA world/tick health is present, no world
  tick timeout is observed, `sensor_capture` is the slowest frame stage, GT
  localization/chassis publish wall rate is about `3.7Hz`, Apollo control RX
  wall rate is about `62Hz`, and control-to-GT count ratio is about `17`.
  The next diagnostic profile is
  `configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_compat.yaml`,
  which disables legacy rig sensor capture through `record.sensors.enable=false`
  while preserving the same Phase 1 Apollo fixed-scene compatibility boundary.
  A positive cadence change from that profile would be diagnostic only; it
  would not prove Apollo natural driving or justify smoothing/clamping bridge
  output.
- The low-capture online run
  `runs/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_compat`
  confirms that `record.sensors.enable=false` disables legacy rig sensor
  capture and materially improves wall-clock cadence: tick p95 drops from about
  `0.286s` to about `0.0082s`, localization/chassis wall rate rises from about
  `3.7Hz` to about `19.8Hz`, and the control-to-GT count ratio falls from about
  `17` to about `3.4`. It still fails with `lane_invasion`, and
  `control_health` still reports `apollo_raw_command_oscillation`. The Apollo
  link-health primary blocker initially shifted to missing scenario route
  fields. After route-contract postprocess consumes runtime
  `artifacts/scenario_metadata.json` front alignment, the same run records a
  `300m` fixed-scene route trace and CARLA lane signature `0:-2`. Exporting
  official Apollo HDMap API projection rows for the same run produces 80/80 ok
  rows over about `301.4m` of projection-s coverage; this moves
  `apollo_route_contract` from `insufficient_data` to `warn` rather than pass,
  because CARLA lane `0:-2` and Apollo lane `0_0_2` remain cross-namespace and
  require explicit equivalence evidence. The refreshed link-health primary
  blocker moves downstream to
  `channel_health:localization:header_sim_gap_over_contract`. This is
  diagnostic progress only; it is not a driving improvement and does not justify
  bridge smoothing or PID tuning before the localization/channel timing contract
  is explained.
- The refreshed channel-cadence diagnosis now records `sim_wall_cadence`. In the
  same low-capture run, CARLA advanced about `87.3s` of sim-time in about
  `11.1s` of wall-time while `wall_time_pacing_enabled=false`. This explains
  why localization/chassis wall delivery can be near `20Hz` while Apollo header
  sim-time cadence is only about `2.5Hz`. The diagnostic follow-up config is
  `configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml`.
  It enables `run.wall_time_pacing.enabled=true` at `0.05s` and exists only to
  test whether pacing restores the sim-time channel contract. A paced-positive
  run would still be diagnostic; it would not prove Apollo natural driving or
  make the overlay/low-capture profile default.
- The paced online sample
  `runs/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat`
  confirms the channel-timing hypothesis: `sim_wall_speedup_factor≈0.98`,
  localization/chassis header sim cadence about `19.1Hz`, max gap about
  `100ms`, and `channel_cadence_diagnosis.status=warn` with no blocking
  reasons. The same run still fails as `phase1_status=failed` /
  `failure_reason=lane_invasion`. After official HDMap projection export,
  `apollo_hdmap_projection.status=pass` / `claim_grade=true`,
  `apollo_reference_line_contract.status=warn`, and link-health primary blocker
  moves to `apollo_module_consumption:claim_route_consumption_unverified`.
  This is a blocker shift, not a behavior improvement claim.

Implementation note added after the 2026-06-16 review:

- `carla_testbed/scenario_player/target_actor.py` provides the first explicit
  target actor resolver and writes `target_actor_contract` into compiled
  fixed-scene storyboards.
- `carla_testbed/analysis/gap.py` defines the first bumper-to-bumper gap
  utility. Center-distance fallback is marked degraded.
- `carla_testbed/analysis/v_t_gap.py` extracts Phase 1 `v-t-gap` artifacts from
  `timeseries.*` and `scenario_actor_trace.jsonl`. Its CSV surface records
  `gap_degraded_reason` and route/lane metadata so a review can distinguish
  bbox-based bumper gap, same-lane `route_s_bumper_gap`,
  fixed-scene `trajectory_progress_bumper_gap`, and diagnostic fallback or
  degraded projection gap.
- `carla_testbed/analysis/phase1_status.py` centralizes
  `success/degraded/failed/invalid` run status classification. Preflight/setup
  invalid reasons such as `backend_not_ready` take priority over missing raw
  traces, so an Apollo scaffold without runtime artifacts is not rewritten as a
  behavior failure or plain `no_timeseries`.
- `carla_testbed/analysis/scenario_comparison.py` writes the first generic
  ScenarioComparison report. Combined `v-t-gap` curves are written as
  `comparison_curves/v_t_gap_combined.csv` and include only evaluable runs;
  invalid runs remain visible in the manifest/summary but are excluded from
  behavior curves.
- `carla_testbed/analysis/phase1_postprocess.py` closes the run-local Phase 1
  artifact spine by writing `analysis/v_t_gap/`,
  `analysis/phase1_status/`, and artifact completeness for a completed
  ScenarioRun. When rerun on an existing run directory, it refreshes only the
  Phase 1 echo fields in `summary.json`; it does not rewrite runtime
  `status/success` fields.
- `carla_testbed/experiments/phase1_scenario_binding.py` can now bind both
  fixed-scene and route-only scenario specs to legacy/compat run directories.
  Route-only binding writes `target_actor_contract.status=not_required` and
  does not create `fixed_scene_resolved.json`, so it can identify cases such as
  `town01_junction_031` without inventing fixed-scene evidence.
- `carla_testbed/analysis/phase1_scenario_catalog.py` writes a read-only
  P0/P1 catalog that separates case YAML, template compile, CARLA online,
  Apollo online, `v-t-gap`, comparison evidence, and optional `--evidence-root`
  ingestion. YAML or template evidence alone never marks a scenario DONE. The
  catalog also emits per-scenario `next_action` hints derived from missing
  evidence and representative Apollo blockers; these are triage prompts, not
  readiness gates.
- `StackContract.to_dict()` now emits manifest-compatible `backend_name`,
  runtime start flags, input contract, output control mode, and available truth
  fields so Phase 1 runs can be audited without guessing backend semantics.
- `tools/run_phase1_scenario.py` now writes a structured Apollo fixed-scene
  preflight scaffold. When runtime is not migrated, it records
  `invalid/backend_not_ready` instead of producing an empty launch plan or
  silently counting Apollo as a behavior loss.
- `carla_testbed/analysis/scenario_comparison.py` now records
  `comparison_target_status` and `backend_coverage`. A same-backend regression
  and an ApolloBackend-vs-PlanningControlBackend comparison are no longer
  conflated.

These additions improve the offline Phase 1 evaluation spine. They do not by
themselves prove online CARLA playback, Apollo fixed-scene compatibility, or
backend behavior quality.

## Important Current Findings

Latest local validation snapshot, `2026-06-20`:

- Full CI-friendly pytest passed locally before the hard-brake online update:
  `1885 passed`. After the hard-brake run, the targeted validation was the
  online Apollo sidecar run, refreshed builtin run, comparison generation, and
  catalog refresh below.
- Local CARLA `carla_builtin` diagnostic samples now exist for all current
  P0/P1 catalog rows. Fixed-scene target cases produce `v_t_gap_status=pass`;
  route-only lane/curve/junction cases produce `v_t_gap_status=not_applicable`
  with `target_actor_contract.status=not_required`.
- Representative new local run evidence includes:
  `runs/phase1_online_builtin_lane_keep_safety_surface_20260619_163934/`,
  `runs/phase1_online_builtin_curve217_diagnostic_20260617_135025/`,
  `runs/phase1_online_builtin_junction_031_20260617_135035/`, and
  `runs/phase1_online_builtin_cut_out_20260617_134507/`, plus
  `runs/phase1_builtin_lead_accel_decel_097_goal_cycle_20260618_031551/`
  for the current lead speed-change target case.
- Apollo scaffold comparisons and real Apollo-vs-builtin comparison candidates
  now exist for all current P0/P1 catalog rows, including:
  `runs/phase1_comparisons/lane_keep_real_apollo_vs_builtin_safety_surface_20260619_164004/`,
  `runs/phase1_comparisons/follow_stop_097_apollo_vs_builtin_20260618_030308/`, and
  `runs/phase1_comparisons/curve217_apollo_cadence_hotpath_validation2_vs_builtin_20260619_180900/`,
  `runs/phase1_comparisons/junction031_apollo_online_lateral_20260619_181629_vs_builtin_20260619_181912/`,
  `runs/phase1_comparisons/baguang_lead_decel_apollo_overlay_vs_builtin_safety_surface_20260619_210445/`,
  `runs/phase1_comparisons/baguang_lead_hard_brake_apollo_overlay_vs_builtin_safety_surface_20260620_004757/`,
  `runs/phase1_comparisons/baguang_cut_in_apollo_overlay_vs_builtin_safety_surface_20260620_005828/`,
  and
  `runs/phase1_comparisons/baguang_cut_out_apollo_overlay_vs_builtin_safety_surface_20260620_010954/`.
  These comparisons are `comparable` with
  `comparison_target_status=apollo_vs_planning_control_evaluable`, but they are
  not automatically accepted Phase 1 bundles.
- The previous generated catalog reading of `8` DONE scenarios was too
  optimistic because it treated best available readiness/comparison evidence as
  completion. The current acceptance gate keeps rows `PARTIAL` until a
  `phase1_acceptance_report.json` binds one exact Apollo run, one exact
  PlanningControl run, and one exact comparison with all validity gates true.
  The hard-brake Apollo sidecar
  run `runs/phase1_apollo_sidecar_lead_hard_brake_overlay_online_20260620_004446`
  and refreshed builtin run
  `runs/phase1_builtin_baguang_lead_hard_brake_safety_surface_20260620_004737`
  are both evaluable but failed with `lane_invasion`. The cut-in Apollo sidecar
  run `runs/phase1_apollo_sidecar_cut_in_overlay_online_20260620_005245` and
  refreshed builtin run
  `runs/phase1_builtin_baguang_cut_in_safety_surface_20260620_005800` are also
  evaluable `failed/lane_invasion` runs; the Apollo cut-in failure occurs
  before target activation. The Baguang cut-out accepted bundle
  `runs/phase1_acceptance_cut_out_simple_20260620_1622/acceptance/phase1_acceptance_report.json`
  binds the corresponding Apollo sidecar run and builtin run with all Phase 1
  acceptance gates true after recognizing explicit lane-change trace completion
  evidence. Both participating runs still failed with `lane_invasion`, so this
  proves evaluability and matched evidence surfaces, not behavior success.
- The generated catalog now separates Apollo fixed-scene bridge-config
  readiness, Apollo fixed-scene dispatch-contract evidence, and Apollo
  fixed-scene runtime dispatch. A readiness report can be `DONE` while the
  dispatch contract is `NOT_YET` or the runtime dispatch is `PARTIAL`; those
  remain setup/evidence blockers, not Apollo behavior losses.

Implementation note added after the 2026-06-17 continuous loop:

- New Phase 1 scaffold and builtin manifest paths write `fixed_scene_case`,
  `target_actor_contract`, and `artifact_contract_version`. These are input
  contract fields, not behavior success evidence.
- `phase1_status.json` now echoes the target actor and artifact contract used
  for classification. Resolved/v-t-gap evidence overrides static manifest
  intent, so a later `missing_target_actor` finding is not hidden by manifest
  defaults.
- `phase1_scenario_catalog.json` now lists concrete `scenario_case_ids`,
  `case_files`, and `target_actor_contract` alongside readiness fields. It
  remains an orchestration summary and only marks a scenario DONE after online
  ApolloBackend and PlanningControlBackend comparison evidence is present.
- P1 `lead_hard_brake` now has a Baguang ScenarioCase YAML, compiler test,
  online Apollo sidecar evidence, online `carla_builtin` evidence, and a
  comparable ApolloBackend-vs-PlanningControlBackend report. Its representative
  runs are evaluable failures, so this is scenario-platform readiness, not a
  capability pass.
- `cut_in_simple` now has Baguang Apollo sidecar evidence, refreshed
  `carla_builtin` evidence, native Prediction evidence from
  `artifacts/apollo_planning.data`, preserved runtime lane-invasion marking
  evidence in root `events.jsonl`, and a self-contained
  `phase1_acceptance_report.json` bundle. The scenario starts inside the target
  activation window, so target transition is exercised. The latest online sample
  includes `crossed_lane_marking_types=["Solid"]`, `scenario_actor_contract=pass`,
  and `v_t_gap=warn`. The Apollo `v_t_gap` warning now comes from late
  lateral-offset degradation, not from timebase mismatch: the extractor aligns
  relative fixed-scene actor trace rows to absolute sidecar timeseries using
  `fixed_scene_runtime_hook.start_sim_time_s`. The Baguang lane-event contract
  uses a yaw-aware vehicle footprint, so the Apollo sample's lane-invasion event
  is hard-gate-eligible and classified as
  `possible_real_lane_departure_or_unclassified_lane_event`. The paired
  `carla_builtin` runner now attaches safety sensors after the ego-spawn
  settling tick, so spawn-time lane-invasion callbacks are excluded from run
  behavior evidence. The refreshed builtin run has `lane_invasion_count=0`, the
  comparison is `comparable/apollo_vs_planning_control_evaluable`, and the
  acceptance bundle is `DONE`.
- `cut_out_simple` now has a Baguang fixed-scene ScenarioCase,
  `configs/scenarios/baguang/cut_out_35kph_right_to_left_25m.yaml`, online
  Apollo sidecar evidence, online `carla_builtin` evidence, a comparable
  ApolloBackend-vs-PlanningControlBackend report, and a self-contained accepted
  `phase1_acceptance_report.json` bundle. Its representative runs are
  evaluable failures, so this is accepted Phase 1 comparison-surface evidence,
  not a cut-out behavior pass.
- Current catalog completion is governed by self-contained accepted bundles,
  not by the old cross-run readiness snapshot. Current P0/P1 catalog progress
  is `8 DONE / 0 PARTIAL`; this means the current accepted comparison-surface
  rows are complete. It does not mean every backend behavior passed.
- `carla_builtin` route-only scenarios now write Phase 1 artifacts without
  fixed-scene actor files. The postprocess spine treats their `v-t-gap` report
  as `not_applicable`, not `invalid`.

DONE findings:

- Fixed-scene offline compiler/storyboard construction has code, docs, and unit
  tests.
- Backend facades exist and can be listed without importing runtime stacks.
- `carla_builtin` can build a diagnostic launch plan for Baguang static
  follow-stop.
- Standard artifact writers exist for surfaces such as `manifest.json`,
  `summary.json`, `events.jsonl`, and `timeseries.jsonl`.

PARTIAL findings:

- CARLA online playback exists for current P0/P1 rows through `carla_builtin`,
  but those runs are diagnostic PlanningControlBackend evidence only. They are
  not Apollo/Autoware capability evidence and do not make a Phase 1 row DONE
  without the paired ApolloBackend run, ScenarioComparison, and self-contained
  acceptance bundle.
- Apollo backend is present as a metadata facade and bridge compatibility path.
  One legacy `follow_stop_097` Apollo run is now cataloged as evaluable
  `failed/unsafe_gap`, while most fixed-scene Apollo rows are still scaffold or
  readiness-only invalid evidence. Fixed-scene Apollo runtime dispatch is not
  yet fully migrated behind the facade.
- The self-written controller exists as `carla_builtin` /
  `simple_acc_route_follower`; `PlanningControlBackend` is the Phase 1 target
  category, not a reason to rename current runtime modules.
- Generic `ScenarioCase x Backend` comparison artifacts exist. `lane_keep_097`,
  `follow_stop_097`, lead-decel, hard-brake, cut-in, cut-out, curve, and
  junction now have real Apollo-vs-builtin comparable reports.
  The refreshed lane-keep builtin run includes explicit collision/lane-invasion
  counter surfaces, so the comparison is no longer blocked by
  `safety_event_evidence_mismatch`. `follow_stop_097` is comparable because the
  builtin run is now classified as evaluable
  `failed/scenario_phase_trigger_not_reached` instead of invalid setup failure.
  Current fixed-scene cut-in/cut-out representative runs are comparable but
  behaviorally failed, so the next work is blocker reduction, not more
  readiness accounting.
- Apollo fixed-scene target-actor runs are invalid if the target actor is not
  proven to enter Apollo perception through `obstacle_gt_contract` evidence.
  This keeps a missing lead/cut-in obstacle from being counted as an Apollo
  backend behavior loss.
- ScenarioComparison now records active/blocking assists from run artifacts.
  Explicit blocking assists keep the run visible as assisted evidence but
  prevent the comparison from satisfying the Phase 1 reference-backend target;
  assisted failures are not counted as reference-backend losses.
- Fixed-scene playback validity includes scenario actor behavior contracts, not
  just artifact presence and phase start/completion. If
  `analysis/scenario_actor_contract/scenario_actor_contract_report.json` fails,
  Phase 1 classification is `invalid/fixed_scene_failed` before considering
  `v-t-gap` unsafe-gap evidence. This keeps a non-ego actor playback failure,
  such as a lead vehicle missing its speed profile, from being counted as a
  backend behavior loss.
- The latest local `lead_accel_decel_097` builtin online sample
  (`runs/phase1_builtin_lead_accel_decel_097_goal_cycle_20260618_031551/`) now
  produces complete artifacts, valid fixed-scene playback, valid scenario actor
  speed-profile evidence, and `v-t-gap`. Same-lane rows use
  `route_s_bumper_gap` only when CARLA lane-s agrees with the actor-trace
  forward anchor; rows with `route_s_anchor_conflict` or lane/road transitions
  use `trajectory_progress_bumper_gap`, so `v_t_gap.status=pass`. This run is
  Phase 1 `success` with final gap around 25.5m and minimum gap around 16.1m.
  This proves the `PlanningControlBackend` diagnostic path can exercise the
  speed-change ScenarioCase with an evaluable target-gap surface; it is not
  Apollo/Autoware capability evidence. The next work is same-ScenarioCase
  Apollo compatibility evidence and, for natural-driving claims, stronger
  route/Frenet target-gap evidence across lane transitions.
- Phase 1 summary semantics, event vocabulary, and docs claim consistency are
  partially defined but not yet a complete evaluator contract.

NOT_YET findings:

- No current P0/P1 scenario catalog row remains `NOT_YET`. New scenarios should
  still start as `NOT_YET` until they have YAML, target/gap semantics, online
  backend evidence, and comparison evidence.

New PARTIAL findings from the offline Phase 1 spine:

- Dedicated `v-t-gap` extraction exists with schema `v_t_gap.v1`. Local
  builtin online runs can produce it, but online Apollo-vs-builtin comparison
  curves are still missing.
- Degraded gap methods are now represented separately from behavioral large
  final gap outcomes. A run that only has center-distance or legacy lead-gap
  fallback can be evaluable/degraded, but the fallback is not claim-grade
  bumper-to-bumper gap evidence.
- Longitudinal projection gap is also degraded when the target's lateral offset
  exceeds the projection-validity boundary. In that case Phase 1 status may be
  `degraded/degraded_gap_method`; it must not convert a degraded negative
  projection into `failed/unsafe_gap`.
- Same-lane `route_s_bumper_gap` can replace ego-frame projection for rows with
  comparable route/lane metadata. CARLA lane-local `waypoint.s` direction is
  signed by an explicit forward/longitudinal anchor and the report records
  whether direction correction was applied. Rows with lane-id mismatch or
  route-s/anchor conflict do not mix lane-local `route_s`; they either use
  fixed-scene trajectory-progress evidence or remain degraded if progress
  evidence is insufficient.
- `trajectory_progress_bumper_gap` is now available as Phase 1 fixed-scene
  observed gap evidence for lane/road transitions. It can make a ScenarioRun
  evaluable for longitudinal behavior, but it remains separate from
  claim-grade Apollo HDMap/Frenet projection evidence.
- Bumper-to-bumper gap calculation exists and `carla_builtin` fixed-scene
  traces now write CARLA bbox-derived vehicle dimensions when CARLA exposes
  them. Old traces without dimensions remain degraded diagnostic evidence.
- Central Phase 1 run status/failure-reason classification exists, and the
  `carla_builtin` fixed-scene runner invokes it automatically after each run.
  Legacy and Apollo online paths still need explicit integration.

UNKNOWN findings:

- Whether all legacy online run paths already write the new manifest-level
  backend input contract fields. New Phase 1/builtin paths do; older tools may
  differ.
- Whether existing CARLA online builtin runs consistently produce complete
  fixed-scene artifacts on every host. The code path exists, but local runtime
  evidence is host-dependent.

## Suggested Phase 1 PR Sequence

1. Add a read-only Phase 1 scenario catalog summary for P0/P1 cases.
2. Add target-role resolver.
3. Add bumper-to-bumper gap utility with vehicle-length inputs and fixtures.
4. Add minimal `v-t-gap` extractor using existing ego timeseries and scenario
   actor trace.
5. Add Phase 1 status/failure classifier that maps existing run artifacts to
   `success`, `degraded`, `failed`, or `invalid`.
6. Add a generic ScenarioComparison report that can initially wrap existing
   Baguang comparison evidence.
7. Add one Apollo fixed-scene compatibility path only after the artifact and
   invalid-run boundaries above are stable.
