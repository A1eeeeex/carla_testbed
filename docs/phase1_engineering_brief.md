# Phase 1 Engineering Brief

Last reviewed: 2026-07-19

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

Latest reviewed snapshot: `2026-07-19`. The latest complete five-P0 matrix is
`runs/phase1_p0_matrix/cycle_inf_mainline_vehicle_param_map_speed_20260714`.
All five pairs are materialized, evaluable, and comparable with no invalid
rows. PlanningControlBackend succeeds in all five rows. Apollo succeeds in the
two Town01 route rows and has three valid behavior failures: lane invasion in
`follow_stop_static`, and collision in `lead_decel_accel` and
`cut_in_simple`. The Town01 `lane_keep_curve` row remains a terminal-turn
diagnostic route and is not claim-grade curve evidence. This matrix replaces
the 2026-07-01 matrix as the Phase 1 comparison-surface baseline; it does not
support an Apollo natural-driving claim.

Layered progress status:

- Phase 1 overall: `PARTIAL`
- Code-level platform delivery estimate: about `88-90%`
- Current repo independently provable completion: about `84-88%`
- Platform skeleton estimate: about `90%`
- Apollo reference runtime estimate: about `78-82%`
- Five-P0 online pair matrix estimate: `DONE for evaluable comparison surface; Apollo behavior succeeds in 2/5 rows`
- Local accepted comparison-surface catalog: `PARTIAL after verifier hardening`
- Package-level acceptance surface: `PARTIAL until rebuilt from verifier-passed bundles`
- Latest external Pro-audit completion verdict: `PARTIAL / Phase 1 done claim rejected`
- Phase 1 completion gate: `PARTIAL`
- Representative Apollo behavior capability: `PARTIAL / latest complete matrix has two Town01 successes and three Baguang failures; targeted cut-in short-handover success is repeated only under quarantined bidirectional steering alignment, while the matched legacy-steering row is an evaluable lane-invasion failure`
- Apollo natural-driving or no-interference capability claim: `NOT_CLAIMED`

2026-07-19 targeted online evidence:

- The atomic initial-readiness gate and the post-transition compute budget are
  now separated. The opt-in native-Prediction candidate requires the latest
  eligible Planning message itself to be a fresh NORMAL replan before the
  one-shot authored-speed materialization. This prevents a later Routing
  success from being combined with an aging stitched continuation; after
  materialization, the existing distinct replan plus non-replan NORMAL
  confirmation contract remains unchanged. Python compile, `148` focused
  tests, and full pytest (`2471 passed`) cover the shared-code change.
- Online v62
  `runs/phase1_online_pairs/cycle_inf_baguang_native_prediction_replan_gate_physical_brake_v62_20260719`
  proves the initial boundary but not handover. Routing plus Planning sequence
  `54`, a `111`-point NORMAL replan, open the gate at sim `17.797s` while
  ego/lead remain approximately stationary and formal time is false. After
  speed materialization, however, Planning falls to about `2.5Hz` in simulation
  time, its fallback trajectory decays to one point, Control tx remains zero,
  and the Apollo row is correctly `invalid/setup_failed` rather than a backend
  loss.
- v33/v62 runtime evidence identifies why the older v33 recovered while v62
  did not. v33's old readiness path accidentally spent p95 about `0.768s` per
  tick in `after_world_tick`, so its configured `50ms` pacing almost never
  slept and incidental artifact I/O slowed CARLA enough for CPU Prediction.
  v62 removes that hot-path cost: inter-tick p95 becomes about `0.054s`, while
  `79` measured CPU VectorNet inferences still take `384-559ms` with p95 about
  `470ms`. The old success therefore depended on accidental I/O backpressure,
  not an adequate explicit compute-time contract.
- A default-off compute-paced candidate allocates `250ms` wall time per `50ms`
  simulation tick; two ticks cover the measured VectorNet p95 without changing
  Planning, Prediction semantics, scenario simulation time, mapping, steering,
  guards, readiness, or Town01. Config validation and `26` profile tests pass.
  Online pair
  `runs/phase1_online_pairs/cycle_inf_baguang_native_prediction_compute_paced_v63_20260719`
  materializes the requested pacing on `619/620` ticks with inter-tick p95
  about `0.253s` and restores both Prediction and Planning to median `0.10s`
  simulation-time cadence.
- v63 closes this runtime lifecycle in one online sample. Routing succeeds at
  sim `13.867s`; the gate opens at `14.117s` on Planning sequence `71`, a
  `111`-point NORMAL replan, with pre-materialization ego/lead speeds about
  `1.43e-6/5.72e-7m/s` and formal time still false. Nine setup-only
  SPEED_FALLBACK messages follow the state transition, NORMAL recovers at
  `+1.0s`, and exact handover uses non-replan NORMAL sequence `115` with `116`
  points. Control tx is `0` before handover and `1` at acknowledgement; formal
  time begins at sim `18.667s` with ego/lead about `19.553/19.552m/s` and gap
  `20.004m`.
- Both v63 backend rows are completed/evaluable `success`, and the comparison
  is `comparable / apollo_vs_planning_control_evaluable`. Apollo is runtime-
  aligned, has zero collision and lane invasion, and all `181` formal Planning
  messages are NORMAL. Its `361` valid gap rows have minimum about `20.001m`
  and final about `45.592m`; final ego/lead speed is about
  `16.532/19.184m/s`. This proves the compute-paced readiness/handover path and
  one Phase 1 behavior success. It remains a default-off extended-map
  diagnostic with Control health `warn`; it does not replace the canonical P0
  matrix, establish repeatability, promote physical mapping, or support an
  Apollo natural-driving claim.
- Exact repeat v64
  `runs/phase1_online_pairs/cycle_inf_baguang_native_prediction_compute_paced_repeat_v64_20260719`
  repeats the strict lifecycle but not the behavior result. Routing and a
  fresh multi-point NORMAL replan still precede one-shot speed
  materialization; exact compatible NORMAL handover and zero pre-handover
  Control tx remain intact. Both rows complete and are evaluable, the pair is
  comparable, and there is no collision or lane invasion. Apollo is correctly
  `degraded/large_final_gap`: final gap grows to about `83.199m`, final ego
  speed falls to about `13.216m/s`, and formal Planning is `180 NORMAL / 1
  SPEED_FALLBACK` rather than v63's `181/181 NORMAL`.
- The v64 formal fallback is attributable to the declared simulator-vehicle
  acceleration contract. At formal `+8.65s`, measured CARLA feedback enters a
  replan at about `9.188m/s` and `2.049083m/s^2`, just above the active Apollo
  vehicle parameter `max_acceleration=2.0m/s^2`. Piecewise Jerk Speed reports
  OSQP primal infeasibility under acceleration bounds `[-6,2]` and emits
  SPEED_FALLBACK. The matched v63 phase remains below the bound at about
  `1.454409m/s^2` and stays NORMAL. This is a Planning initial-state/vehicle-
  capability mismatch, not a steering-unit, road-length, readiness, or
  Prediction-cadence regression.
- A default-off candidate changes only the Apollo simulator vehicle parameter
  `max_acceleration: 2.0 -> 4.0m/s^2`. It preserves measured/unfiltered CARLA
  localization acceleration, strict Routing/NORMAL replan readiness, exact
  handover, steering-only physical calibration, legacy longitudinal output,
  disabled guards, `steer_scale=1.0`, and all Town01 profiles. The candidate
  and resolved runtime shell are covered by `100` focused tests; no shared
  runtime code changes are part of this candidate.
- Online v65
  `runs/phase1_online_pairs/cycle_inf_baguang_compute_paced_vehicle_accel4_v65_20260719`
  materializes the candidate: the generated Apollo module script contains
  `max_acceleration: 4.0000`, and Planning logs acceleration bounds `[-6,4]`.
  The gate opens on Routing plus Planning sequence `71`, a `111`-point NORMAL
  replan, while ego/lead remain approximately stationary and formal time is
  false. Exact handover uses sequence `115`, a `116`-point NORMAL trajectory,
  with zero Control tx before acknowledgement; only then do official scenario
  time and Apollo ownership start.
- Both v65 rows are completed/evaluable Phase 1 `success`; the pair is
  comparable, Apollo is runtime-aligned, collision/lane-invasion counts are
  zero, and formal Planning is `181/181 NORMAL`. All `361` v-t-gap rows are
  valid, with minimum/final gap about `20.001/46.552m` and final ego/lead speed
  about `16.514/19.184m/s`. Nine SPEED_FALLBACK messages remain confined to
  the setup state transition. This is positive single-sample A/B evidence,
  but the exact v64 `2.049083m/s^2` input did not recur at the corresponding
  v65 replan, so it does not yet justify promoting `4.0m/s^2` as the default
  vehicle contract.
- Exact candidate repeat v66
  `runs/phase1_online_pairs/cycle_inf_baguang_compute_paced_vehicle_accel4_repeat_v66_20260719`
  independently repeats the same strict boundary. The gate opens at sim
  `16.077s` on Routing plus sequence `71`, a `111`-point NORMAL replan, while
  both actors are approximately stationary and formal time is false. Exact
  handover uses sequence `115`, a `116`-point NORMAL trajectory, with Control
  tx `0 -> 1`; formal time starts at sim `20.627s` with ego/lead about
  `19.553/19.552m/s` and gap about `20.005m`.
- v66 again has both rows completed/evaluable Phase 1 `success`, a comparable
  pair, runtime alignment, and zero collision/lane invasion. Its full Planning
  distribution exactly matches v65 at `218 NORMAL / 71 UNKNOWN / 9 setup
  SPEED_FALLBACK`; formal Planning again remains `181/181 NORMAL`. All `361`
  gap rows pass, with minimum/final gap about `20.001/45.949m` and final
  ego/lead speed about `16.537/19.184m/s`. The v65/v66 final-gap spread is only
  about `0.604m`, and final ego-speed spread is about `0.024m/s`, closing
  candidate outcome repeatability for this extended-map diagnostic.
- The promotion boundary remains open. v66's formal `+8.65s` replan is NORMAL
  at about `1.731m/s^2`; like v65, it does not present the exact v64
  `2.049083m/s^2` state to the widened solver. Formal frame-level CARLA
  acceleration also still contains short transients above `4m/s^2`, so
  `4.0m/s^2` is not yet a source-backed portable vehicle envelope. Broader
  vehicle/gear-transition calibration and canonical-map regression remain
  required before any default change.
- Current-state Prediction v67
  `runs/phase1_online_pairs/cycle_inf_baguang_compute_paced_vehicle_accel4_current_state_prediction_v67_20260719`
  is negative setup evidence, not a lateral-control regression. The initial
  `apollo_planning_ready` gate still opens on Routing plus sequence `71`, a
  `111`-point NORMAL replan, while both actors are stationary. After speed and
  drivetrain stabilization, Apollo emits many fresh, current-speed-compatible
  NORMAL continuations, but the second-stage handover additionally waits for
  `is_replan=true`. No qualifying replan arrives before the setup timeout;
  `control_tx_count=0`, the Apollo row is correctly `invalid/setup_failed`,
  and the pair is only partially evaluable. Its `285 NORMAL / 71 UNKNOWN / 16
  SPEED_FALLBACK` distribution must not be counted as backend behavior or
  steering evidence.
- Default-off v68
  `runs/phase1_online_pairs/cycle_inf_baguang_current_state_post_stability_handover_v68_20260719`
  first showed that removing the post-stability `is_replan` wait permits
  handover, but pre-push self-review found that the same legacy flag also
  controlled the initial gate. The run did observe Routing plus a stationary
  multi-point NORMAL replan before actor motion, but it did not prove that the
  configured gate would reject a non-replan initial trajectory.
- Both v68 rows are completed/evaluable Phase 1 `success`, the pair is
  comparable, and Apollo records zero collision/lane invasion. Full Planning
  is `212 NORMAL / 69 UNKNOWN / 5 setup SPEED_FALLBACK`; all formal behavior
  rows remain safety-event-free. Apollo's `361` valid gap rows have
  minimum/final gap about `18.644/35.364m`, with final ego/lead speed about
  `15.785/19.184m/s`. The frozen steering chain is unchanged: Apollo
  `steering_target` is decoded once from percent, `steer_sign=-1`,
  `steer_scale=1`, and steering-only physical inverse calibration is used.
  CTE absolute p95 is about `0.00237m`, raw Apollo steer absolute p95 about
  `0.000324` in normalized full-scale units, and applied CARLA steer absolute
  p95 about `0.000149`; no lateral guard fires. This closes the specific v67
  replan-anchor starvation blocker and shows v67 was not a repeated lateral
  failure. It does not promote current-state Prediction, the `4m/s^2` vehicle
  candidate, physical mapping defaults, or an Apollo natural-driving claim.
- The corrected v69 pair
  `runs/phase1_online_pairs/cycle_inf_baguang_split_replan_contract_v69_20260719`
  splits the contract into
  `planning_ready_require_initial_replan_anchor=true` and
  `scene_preroll_planning_require_replan_anchor=false`. Effective config and
  runtime artifacts materialize both values. The initial gate records
  `initial_replan_anchor_required=true` and opens at sim `12.581s` on sequence
  `68`, a fresh NORMAL replan, while ego/lead remain approximately stationary.
  Post-stability handover completes on two distinct compatible NORMAL messages,
  with the last sequence `101` explicitly non-replan; Control tx changes
  `0 -> 1` and formal time starts at sim `15.931s`.
- Both v69 rows are completed/evaluable `success`, the pair is comparable,
  and Apollo has zero collision/lane invasion. Planning is `210 NORMAL / 68
  UNKNOWN / 5 setup SPEED_FALLBACK`; gap minimum/final is about
  `18.585/35.115m`, with final ego/lead speed about `15.769/19.184m/s`.
  CTE absolute p95 remains about `0.00241m` with no lateral configuration or
  guard change. This is the online proof for the split lifecycle contract;
  v68 remains supporting behavior evidence only. The split implementation
  passes `108` focused tests and full pytest (`2475 passed`).
- Exact current-state repeat v70
  `runs/phase1_online_pairs/cycle_inf_baguang_split_replan_contract_current_state_repeat_v70_20260719`
  repeats both lifecycle stages. The stationary initial gate opens on sequence
  `72`, a fresh `111`-point NORMAL replan, with strict initial-replan evidence
  materialized. Post-stability handover counts two compatible NORMAL messages
  and finishes on non-replan sequence `105`; Control tx changes `0 -> 1`
  before formal start. Both rows are completed/evaluable `success`, the pair
  is comparable, and Apollo again has zero collision/lane invasion. Planning
  is `210 NORMAL / 72 UNKNOWN / 5 setup SPEED_FALLBACK`.
- v70 repeats v69's behavior within a narrow spread: final gap is
  `35.237m` versus `35.115m`, final ego speed is `15.771m/s` versus
  `15.769m/s`, and minimum gap is `18.597m` versus `18.585m`. At formal
  `+8/+10/+18s`, selected Prediction and Planning speeds are nearly identical
  between the runs. This closes repeatability for the isolated current-state
  diagnostic, not its promotion boundary.
- Matched native-Prediction v32/v33 identify the first upstream behavior
  divergence. At formal `+8s`, the lead still travels at `13.123m/s`, but the
  selected VectorNet trajectory predicts about `1.29/1.37m/s` at `+3s`;
  Planning then commands an approximately `9.34/9.38m/s` one-second speed.
  By formal `+10s`, the actual lead remains near `10.773m/s`, while Apollo's
  ego is nearly stopped and gap has already grown to `43.03/43.22m`. In
  v69/v70, the COST plus MOVE_SEQUENCE candidate holds the selected trajectory
  at current lead speed; ego remains near `8.62/8.61m/s` and gap near
  `19.19/19.18m` at `+10s`. Native learned-trajectory over-deceleration is
  therefore the first proven cause of the roughly `108m` final-gap result.
  The remaining roughly `35m` final gap is a separate longitudinal recovery /
  vehicle-envelope blocker; do not promote the default-off Prediction mode or
  treat this as Apollo natural-driving evidence.
- Prediction instrumentation now records every trajectory's index,
  probability, point count, and bounded speed/position samples instead of only
  the highest-probability trajectory. The change preserves all prior selected-
  trajectory fields, is capped at eight trajectories per obstacle, and passes
  `123` focused tests plus full pytest (`2476 passed`). Online diagnostic v71
  `runs/phase1_online_pairs/cycle_inf_baguang_prediction_all_trajectories_v71_20260719`
  repeats completed/evaluable `success`, comparable status, zero collision /
  lane invasion, and v69/v70 behavior (`35.242m` final gap,
  `15.771m/s` final ego speed).
- v71 proves that the second current-state-candidate trajectory is not a
  hidden longitudinal deceleration hypothesis. Both trajectories have the
  same constant longitudinal speed at every recorded horizon. Trajectory `0`
  remains near the lead's lane; trajectory `1` drifts laterally from about
  `y=-2.4m` toward `y=-6.1m`, so Planning retains its ST boundary only to
  about `2.8s` versus `5.9s` for trajectory `0`. Control follows Planning
  closely after the lead accelerates, while Planning itself requests only
  about `0.5-0.93m/s^2`; it therefore does not recover the roughly `2.2m/s`
  speed deficit present at formal `+10s`. The remaining gap is a Planning
  recovery objective / scenario-horizon question, not an unobserved second
  Prediction deceleration or a Control tracking failure.
- The first matched-road attempt v72
  `runs/phase1_online_pairs/cycle_inf_baguang_matched_extended_scenario_v72_20260719`
  does not provide backend behavior evidence. PlanningControl calls CARLA
  OpenDRIVE generation with the ScenarioCase-owned `639.68m` XODR and records
  three consecutive `60s` RPC timeouts. It therefore has no timeseries and is
  correctly non-evaluable/invalid; CARLA is no longer available when the
  Apollo row starts, so that row is also non-evaluable/invalid. Neither row
  counts as a backend loss, and v72 cannot answer the `18s` gap-recovery
  question.
- The next runtime blocker is now OpenDRIVE world materialization stability,
  before further longitudinal tuning. Determine whether CARLA remains busy or
  exits during the first generation request, and whether immediate retries
  against the same server are safe. Do not raise the timeout or alter mesh
  generation parameters without a one-shot reproduction that distinguishes a
  slow successful generation from a terminated server. Keep the existing
  XODR hash and record the load attempt as setup evidence only.
- The v26/v27 native-Prediction gap spread was traced to mixed-age obstacle
  input at the ROS-to-Cyber boundary. The GT publisher emitted odometry before
  the same-tick object snapshot, so the bridge could consume the preceding
  object position and relabel it with the current Localization timestamp.
  v26/v27 each contain `85` published obstacle samples with `0.05s` source age;
  five phase-aligned age mismatches create roughly `0.7-0.97m` one-frame
  position differences that remain in VectorNet's 20-position history.
- Candidate policy `wait_for_exact_source_time` now requires an object snapshot
  with the same source timestamp as the dequeued odometry sample, drops an odom
  sample when a later object timestamp proves that exact data is missing, and
  records wait/missing/drop counters. The producer also publishes objects
  before odometry. The existing latest-not-after policy remains the default;
  stable Town01 profiles are unchanged.
- Two matched-map online pairs,
  `runs/phase1_online_pairs/cycle_inf_baguang_native_prediction_exact_gt_matched_map_v32_20260719`
  and
  `runs/phase1_online_pairs/cycle_inf_baguang_native_prediction_exact_gt_matched_map_v33_20260719`,
  prove the candidate path online. Both backend rows are completed/evaluable,
  both comparisons are comparable, PlanningControl succeeds, and Apollo has
  zero collision/lane invasion. Every one of the `304` published obstacle
  samples in each Apollo run has source age exactly `0.0s`; wait, missing-exact,
  dropped-odom, and overflow counters are all zero.
- Both repeats preserve the strict lifecycle: Routing succeeds before a
  multi-point NORMAL trajectory opens `apollo_planning_ready`; two compatible
  NORMAL messages and Apollo Control precede formal start. Full-run Planning is
  identical at `213 NORMAL / 9 setup SPEED_FALLBACK / 78 UNKNOWN`, while the
  formal `361` v-t-gap rows remain valid and fallback-free.
- Exact alignment closes the run-to-run Prediction variability, but not the
  behavior blocker. v32/v33 end at `107.877/107.942m` gap with final ego speed
  `11.221/11.709m/s` against the same `19.184m/s` lead speed, and both are
  evaluable `degraded/large_final_gap` results. Their native Prediction samples
  and NORMAL Planning speed profiles are nearly identical; near formal
  `+9-10s`, Planning reduces the ego speed profile to nearly zero and recovers
  too slowly after the lead accelerates. This is now the highest-value
  longitudinal follow-up. It is not a lateral-unit failure: both repeats have
  zero lane invasion, with CTE p95 about `0.076/0.123m`.
- A default-off legacy-brake calibration candidate changes only
  `brake_scale: 1.0 -> 0.38` and normalized-CARLA-domain
  `brake_deadzone: 0.05 -> 0.01`; steering, Planning, GT policy, readiness,
  guards, legacy longitudinal ownership, and Town01 remain unchanged. CARLA-
  direct response motivates the ratio, but that probe is diagnostic-only and
  cannot establish Apollo behavior.
- Its v36 online attempt is invalid/incomplete. The Apollo dispatch times out
  after about `11.3s` of formal playback, and two full-brake rows are traced to
  `carla_control_bridge` watchdog writes at frames `1731/1804`, not to Apollo
  raw or mapped brake. The timed-out child process and synchronous-world state
  were cleaned before retry; CARLA was kept running.
- Warm-CARLA v37 proves the strict initial readiness ordering independently of
  behavior success. Routing and a fresh `176`-point NORMAL trajectory precede
  the gate at sim `18.179s`; ego/lead are still approximately stationary and
  official scenario time is false. Motion starts on the following frame.
  After that transition, however, the first Planning message at `+0.45s` is
  SPEED_FALLBACK with `lon_diff=-6.69473m`, all `82` post-gate Planning rows
  remain fallback, and Apollo Control publishes zero commands. PlanningControl
  succeeds, but Apollo is `invalid/setup_failed` and the comparison is only
  `partially_evaluable`. This neither validates the brake-scale candidate nor
  counts as an Apollo behavior loss.
- v37 also separates the startup discontinuity from the finite-road tail. The
  initial fallback appears long before the 640m road is consumed; the later
  destination condition occurs after setup coasts near `19.55m/s` for the
  timeout window. Do not restore scenario throttle control, relax NORMAL
  readiness, or claim that a longer road alone closes this handover.
- A stationary-start follow-stop cross-check isolates the same default-off
  longitudinal scalar without the dynamic `0 -> 70kph` transition:
  `runs/phase1_online_pairs/cycle_inf_baguang_followstop_static_brake_scale_v38_20260719`.
  The pair completes and is comparable; Apollo is evaluable, hands over from
  stationary ego/lead after Routing plus a fresh `111`-point NORMAL
  trajectory, and has `1 UNKNOWN / 737 NORMAL`, zero fallback, zero watchdog,
  zero collision, and zero lane invasion. This is nevertheless a negative
  behavior result: applied-brake maximum falls from the matched native-8m
  baseline's about `0.340` to `0.057`, but minimum/final bumper gap falls from
  `8.163m` to `7.868m`, below the authored `8.0m` threshold. The constant
  `brake_scale=0.38` candidate must remain diagnostic-only and cannot be
  promoted or used to claim the dynamic lead blocker is fixed.
- The next longitudinal-mapping step, when resumed, is source-backed
  speed-binned CARLA brake identification. Do not select another scalar by
  interpolating toward the acceptance threshold, relax the stop-gap contract,
  or enable physical longitudinal mapping by default.
- CARLA-direct diagnostic runs v39/v40 now provide the first isolated Lincoln
  brake-only speed-bin sample without taking control of a Phase 1 scenario
  ego. The calibration tool temporarily spawns its own vehicle, uses exact
  synchronous simulation-duration ticks, destroys only that vehicle, and
  restores the prior asynchronous world settings. It can preserve the tracked
  steering table while regenerating only brake data; its output explicitly
  sets `diagnostic_carla_direct`, Apollo-out-of-loop, and both Phase 1 and
  mapping promotion to false.
- v39 exposed and v40 closes a measurement-statistic defect in the diagnostic
  tool. The old brake metric ignored `brake_effective_window_sec` and used an
  85th percentile over the full sample, so terminal stop spikes up to about
  `24.89m/s^2` dominated the 5-10m/s bin. With a declared `0.8s` effective
  window, onset exclusion, and median forward deceleration, the same commands
  produce a maximum about `3.10m/s^2` in that bin; all five 0-22m/s bins pass
  the current loader quality check and materialize as five brake tables. This
  is measurement evidence only.
- v41/v42 add a `brake=0` coast trial to every main speed bin and record both
  total deceleration and the incremental response above that baseline. Their
  per-command physical measurements are identical, including zero difference
  in coast baselines and incremental-response MAE/p95/maximum. This proves
  deterministic fixed-step repeatability; it does not make every response
  physically usable. The repeated 0-2m/s and 5-10m/s bins contain deterministic
  command-response reversals that the initial `0.35m/s^2` tolerance hid.
- Final-policy v43 tightens the diagnostic monotonic tolerance to `0.1m/s^2`
  and preserves the negative evidence. The loader retains only 3/5 bins:
  2-5m/s, 10-15m/s, and 15-22m/s. It rejects 0-2m/s and 5-10m/s with
  `non_monotonic_incremental_brake_response`. Physical longitudinal mapping
  remains default-off and no Apollo A/B is justified from this incomplete
  table.
- Focused v44 records pre-brake state, per-frame gear, forward speed,
  acceleration, pose, and sample index. It localizes the apparent reversals:
  0-2m/s remains in first gear but stronger commands reach the near-stop
  boundary at `0.50-0.60s`, reducing the old metric's active sample count from
  `15` to `8`; 5-10m/s starts in second gear and crosses `2 -> 0 -> 1` between
  `0.50s` and `0.70s`. The 2-5m/s control bin stays in first gear and remains
  monotonic. The old `0.8s` instantaneous-acceleration median therefore mixed
  different drivetrain and stop phases rather than measuring a common brake
  response interval.
- v45 uses forward-speed delta over the fixed `0.20-0.40s` interval. At the
  start all 30 trials have negative longitudinal acceleration; no trial shifts
  gear or falls below `0.5m/s` before the interval ends. All five bins now have
  coast baselines, monotonic incremental response, and loader-reliable tables.
  Recalculated v44/v45 overlap is identical across all `18` shared trials
  (`MAE=max=0.0`). This closes the diagnostic measurement blocker, not the
  Apollo behavior blocker. The table remains under `runs/`, physical mapping
  remains default-off, and a fixed actuator-tracking validation is required
  before any default-off Apollo pair candidate.
- The independent tracking path now supports axis-scoped validation, an
  isolated self-spawned vehicle with exact simulation-duration ticks, actual
  entry-speed lookup, explicit unavailable/high-clamp mapping evidence, and
  the same declared `0.20-0.40s` forward-speed-delta response metric. It
  refuses an occupied world, destroys only its diagnostic actor, restores the
  prior CARLA settings, and requires nonzero samples, every requested speed
  point, mean error at most `1.0m/s^2`, and maximum point error at most
  `1.0m/s^2` for a brake pass. The earlier mean-only result v46 is therefore
  not promotion evidence: its `0-2m/s` high requests were clamped to the last
  command and one point had `5.160m/s^2` error despite a passing mean.
- In-range repeat
  `runs/diagnostics/cycle_inf_baguang_brake_tracking_in_range_v47_20260719`
  tests each v45 speed bin at its own calibrated minimum, midpoint, and
  maximum. It is a truthful full-range `fail`: 15/15 requests materialize and
  all five speed points are covered, but the `1.8m/s` / bin-maximum /
  `brake=0.15` trial falls from `1.878m/s` at `0.20s` to `0.215m/s` at
  `0.40s`, crossing a sharp stop boundary and measuring `8.318m/s^2` against
  the `2.249m/s^2` target. The other 14 points have maximum error
  `0.241m/s^2`; the 12 points over `2-22m/s` have mean/maximum error about
  `0.029/0.121m/s^2`. This validates the medium/high-speed inverse only and
  rejects the v45 `0-2m/s` table as a continuous-deceleration promotion
  surface. No physical longitudinal default or Apollo pair candidate follows.
- Covered-bin calibration
  `runs/diagnostics/cycle_inf_baguang_brake_covered_bins_v48_20260719`
  closes the false-coverage defect behind that low-speed failure. The main
  brake probe has a `2.0m/s` entry floor, so the old `0-2m/s` rows were all
  collected from approximately `2.05-2.14m/s`, outside the declared bin. The
  calibrator now records this bin as
  `unavailable/entry_speed_floor_outside_bin`, materializes only the four
  actually covered `2-22m/s` bins, and the runtime loader refuses to select a
  nearest brake bin outside its declared speed interval.
- Independent repeat
  `runs/diagnostics/cycle_inf_baguang_brake_covered_bins_tracking_v49_20260719`
  passes all `12/12` calibrated min/mid/max targets over four entry speeds,
  with mean/maximum error about `0.0095/0.0283m/s^2`, zero skips, and complete
  `4/4` speed coverage. This is a scoped medium/high-speed CARLA-direct pass,
  not a full longitudinal contract.
- Dedicated low-speed probes v50-v52 separate the excluded interval into
  explicit rolling, stop, and hold states instead of forcing one continuous-
  deceleration table over `0-2m/s`. At approximately `0.5/1.0m/s`, every
  tested nonzero brake command enters the stop state within `0.05-0.10s`, so
  those rows are excluded from rolling identification. The reliable rolling
  section is explicitly bounded to `1.25-1.75m/s`; the loader will not select
  it outside that interval. The stop section uses `stop_cmd=0.01` only below
  `1.0m/s` and for requests of at least `0.8m/s^2`.
- v51 initially rejected every stationary hold command because CARLA's
  spawn-settling vertical velocity made the 3D speed norm approximately
  `0.168m/s` even though planar displacement was zero. v52
  `runs/diagnostics/cycle_inf_baguang_low_speed_brake_state_contract_hold_planar_v52_20260719`
  measures forward/planar motion for stationary hold while retaining the 3D
  norm as a diagnostic field. It identifies `hold_cmd=0.0`; maximum forward
  speed is below `4e-6m/s` with zero displacement. This is a state-semantic
  correction, not a relaxed threshold.
- Independent low-speed tracking v53
  `runs/diagnostics/cycle_inf_baguang_low_speed_brake_state_tracking_v53_20260719`
  passes all `3/3` states against the v52 artifact. Stationary hold uses
  `brake=0.0` with maximum forward speed about `6.6e-8m/s`; a fresh
  `0.944m/s` stop request maps to `brake=0.01` and reaches the `0.05m/s`
  threshold in `0.05s`; a fresh `1.625m/s` rolling request maps to about
  `0.0232`, drops `0.212m/s` in the declared `0.3s` window, and does not enter
  stop until `0.5s`. This closes the isolated low-speed state-identification
  and tracking blocker, but not the full Apollo longitudinal transition.
  Physical mapping remains default-off and no Apollo pair candidate follows.
- Composite calibration v54
  `runs/diagnostics/cycle_inf_baguang_composite_brake_states_v54_20260719`
  uses the new brake-low-speed-only mode to replace only
  `brake.low_speed` while retaining v48's four reliable `2-22m/s` main bins.
  Canonical JSON hashes of the two main-brake payloads are identical
  (`f24159b8...c42c`). The first composite probe truthfully loads zero rolling
  sections: all five requested sections fail reliability, so the loader
  exposes `(1.0,2.0)m/s` as unavailable rather than selecting a nearest table.
- Dense transition probe v57
  `runs/diagnostics/cycle_inf_baguang_low_speed_transition_dense_v57_20260719`
  expands the measured command range to `0.002-0.5`. It identifies stationary
  hold `brake=0`, independently measured stop `brake=0.002` through the
  declared `1.25m/s` stop boundary, and one reliable rolling section bounded
  to `[1.5,2.0)m/s`. At the `1.25m/s` entry, all `17` nonzero commands enter
  the stop state inside the rolling evaluation boundary; the `1.5m/s` section
  remains rejected as non-monotonic. The resulting `(1.25,1.5)m/s` transition
  gap is explicit and must not be nearest-filled or described as covered.
- Independent composite validation v58
  `runs/diagnostics/cycle_inf_baguang_composite_brake_tracking_v58_20260719`
  passes all `12/12` main-bin min/mid/max targets with mean/maximum error
  `0.0095/0.0283m/s^2`, plus hold/stop/rolling `3/3`. The fresh stop trial
  starts at `1.078m/s`, maps to `brake=0.002`, and stops in `0.10s`; the fresh
  rolling trial starts at `1.800m/s`, maps to about `0.2079`, and produces
  `0.228m/s` drop against the `0.300m/s` target (`0.072m/s` error) without
  stopping inside the `0.3s` window. This validates only materialized states
  and bins, not the uncovered transition gap or an Apollo-in-loop behavior.
  The calibration remains under `runs/`, is CARLA-direct/Apollo-out-of-loop,
  and is ineligible for Phase 1 or mapping promotion.
- Two intervening online attempts exposed an independent map-selection
  regression: a scenario `run.map` override replaced the explicit extended
  `OpenDriveMap` profile and left the initial Localization about `114.42m` from
  the loaded Apollo lane, producing no reference line and only UNKNOWN
  Planning. Removing that override restores profile-owned map selection;
  v32/v33 use `OpenDriveMap` and start about `3.9e-7m` from its lane geometry.
- The implementation passes Python compilation, the current `46` focused
  tracking/control-mapping/calibration tests, and the full CARLA 0.9.16 suite
  (`2468 passed`). This
  evidence does not replace the canonical five-P0 matrix or support an Apollo
  natural-driving claim.

2026-07-18 targeted online evidence:

- Native Apollo Prediction plus the strict Planning-ready lifecycle now
  completes the matched-map Baguang `lead_decel_accel` pair in
  `runs/phase1_online_pairs/cycle_inf_baguang_native_prediction_matched_map_v26_20260718`.
  The preceding v25 attempt is an invalid setup timeout: it records `434`
  Routing-success, multi-point NORMAL observations, but wall-clock receipt age
  `0.451-1.302s` makes all of them stale under the old gate while CPU
  Prediction runs synchronous CARLA slower than real time.
- Readiness now uses the Planning header and current CARLA simulation time
  when both are available, while retaining the wall age as a diagnostic and
  falling back to wall time when sim-time evidence is absent. The `0.25s`
  threshold, Routing requirement, two-point minimum, and rejection of
  `SPEED_FALLBACK`, `PATH_FALLBACK`, and `UNKNOWN` are unchanged. In v26 the
  gate opens on a `136`-point NORMAL trajectory after Routing success;
  materialization, obstacle activation, Apollo Control handover, and formal
  time occur in that order.
- Both v26 rows are completed/evaluable, invalid reasons are empty, and the
  comparison is `comparable`. Apollo's formal window is `180/180 NORMAL`, with
  zero collision/lane invasion; all ten full-run SPEED_FALLBACK rows are
  confined to setup. Gap is valid for all `361` rows and changes from about
  `20.006m` to `48.314m`; final ego/lead speed is about
  `16.514/19.184m/s`. This closes the readiness clock blocker and improves the
  v23 final gap of `70.725m`. v26 alone was still one extended-map diagnostic;
  the exact repeat below strengthens lifecycle repeatability but still does
  not make either run a canonical P0 replacement or Apollo natural-driving
  claim.
- The readiness change passes Python compilation, `44` runtime-hook tests,
  and the full suite (`2439 passed`).
- Exact same-config repeat
  `runs/phase1_online_pairs/cycle_inf_baguang_native_prediction_matched_map_v27_20260718`
  repeats completed/evaluable/comparable status, strict Routing plus
  `111`-point NORMAL readiness before actor activation, runtime alignment,
  Apollo `success`, and zero collision/lane invasion. v26 and v27 have the
  exact same full-run Planning distribution (`210 NORMAL / 10 setup
  SPEED_FALLBACK / 78 UNKNOWN`) and the same formal `180/180 NORMAL` window.
- This establishes repeated lifecycle/no-formal-fallback completion, but not
  stable longitudinal quality. v27 ends at about `54.591m` gap and
  `15.912m/s` ego speed versus v26's `48.314m` and `16.514m/s`. Prediction
  topic evidence places the first material divergence upstream of Planning:
  near-identical lead observations produce materially different selected
  native trajectories, after which Planning emits different NORMAL speed
  profiles. The next blocker is native Prediction trajectory repeatability,
  not steering units, mapping, readiness, or fallback acceptance.

- Tick-aligned ego feedback is now online-proven on matched-map diagnostic
  pair
  `runs/phase1_online_pairs/cycle_inf_baguang_tick_aligned_gt_state_matched_map_v23_20260718`.
  The bridge consumes ego control, physical wheel feedback, acceleration,
  gear, pose and vehicle dimensions from the runner's same-tick
  `carla_gt_state.v2` snapshot rather than per-cycle CARLA RPC. It records
  `3337` successful JSON state reads with zero missing/parse errors, about
  `8.08ms` publish-loop p95, queue maximum `1`, and zero overflow. The blocked
  v18 baseline had about `0.529s` synchronous state-read p95, `0.795s` loop
  p95, queue maximum `64`, and `361` overflows.
- The strict lifecycle boundary is preserved. Routing success plus a fresh
  `111`-point NORMAL trajectory precedes one-shot initial-speed
  materialization while ego/lead are stationary and formal time is false.
  Target-ready handover later uses a fresh `116`-point NORMAL trajectory,
  about `19.439/19.438m/s` ego/lead speed, `20.005m` bumper gap, and zero
  prior Control tx. Fallback is never accepted as readiness.
- Both v23 rows complete and are evaluable, invalid reasons are empty, the
  comparison is `comparable`, and PlanningControl succeeds. Apollo has zero
  collision/lane invasion and `220/291` non-empty Planning messages
  (`212 NORMAL / 8 SPEED_FALLBACK / 71 UNKNOWN`). Vehicle dimensions now
  remain in the JSON evidence path, so all `361` v-t-gap rows are valid
  bumper-to-bumper projections rather than degraded center distances.
- This closes the startup/state hot-path blocker, not longitudinal quality.
  Apollo remains `degraded/large_final_gap`: gap changes from about `20.005m`
  to `70.725m`, ego speed falls to about `3.895m/s`, and final ego/lead speed
  is about `12.544/19.184m/s`. v23 is an extended-OpenDRIVE matched-map
  diagnostic, not a canonical P0 replacement or natural-driving claim. The
  next blocker is formal-window fallback/replan and longitudinal recovery.
- Python compile, `161` focused tests, and the full CARLA 0.9.16 suite
  (`2437 passed`) validate the code path offline; v23 supplies the required
  online evidence.

- Exact Apollo Control input identities are now captured from `InputDebug`
  for Localization, Chassis, and Planning, alongside Control's consumed replan
  identity. In the formal window of
  `runs/phase1_online_pairs/cycle_inf_baguang_control_input_identity_v15_20260718`,
  all `227/227` Control messages have complete input identity. There are `13`
  duplicate exact-input groups; `12` keep the LQR
  `steer_angle_limited` value stable to the analyzer tolerance while final
  `steering_target` changes, with a maximum change of about `5.02` percentage
  points. Apollo source places the stateful digital filter between those two
  fields, while maximum-steer-rate limiting is disabled in the active LQR
  config. This localizes the observed repeated-input drift after the LQR
  prefilter; it does not yet prove that this is the sole initiator of the
  closed-loop failure.
- The v15 pair is completed/evaluable/comparable with no invalid row and a
  successful PlanningControl row. The strict `apollo_planning_ready` gate sees
  Routing success and a fresh `176`-point NORMAL trajectory while ego/lead are
  stationary and formal time is false; exact NORMAL handover occurs with zero
  prior Control tx at about `19.51/19.51m/s` and `20.005m` gap. Apollo remains
  an evaluable `failed/lane_invasion` row at formal about `+11.30s`, with no
  collision and valid gap evidence. This is diagnostic localization, not a
  behavior fix or natural-driving evidence.
- Control's authoritative `latest_replan_trajectory_header` reveals `6`
  formal pre-event replans in v15, while the Planning topic artifact observes
  only `4`; target-point deltas reach about `1.44m` in y and `0.221rad` in
  theta. The same method finds `3` formal replans in matched v12 rather than
  the single topic-captured late replan. Replans remain proven amplifiers, not
  a proven first cause. The next smallest causal experiment should isolate
  repeated exact-input digital-filter updates without changing steering
  units, scale/sign, physical calibration, guards, Planning, or Town01.
- Phase-aware Control diagnosis now separates setup, formal-pre-event, and
  terminal safety-event windows. It also stops treating a repeated Control
  header and Planning sequence alone as unchanged vehicle state; post-LQR
  attribution requires stable complete `steer_angle_limited` input while the
  final `steering_target` changes. Reprocessed v12 formal-pre-event duplicate
  groups have steering range p95/max about `3.79/4.49` percentage points and
  `15` stable-prefilter drift groups; v13 has about `27.24/32.60` and `18`.
- The isolated cutoff-preservation A/B
  `runs/phase1_online_pairs/cycle_inf_baguang_cutoff_preserved_v14_20260718`
  is completed/evaluable/comparable with no invalid row and PlanningControl
  success. Runtime launch evidence materializes the only behavior variable,
  `control_lqr.cutoff_freq: 10 -> 2`, at inherited `ts=0.05s`; the strict
  Routing/NORMAL gate, `116`-point handover trajectory, about
  `19.51/19.51m/s` ego/lead speed, `20.005m` gap, mapping, guards,
  longitudinal ownership and Town01 remain unchanged.
- The candidate reduces formal-pre-event steering drift p95/max to about
  `1.90/3.17`, but it is behavior-negative: lane invasion moves from v12
  formal `+10.75s` to `+7.10s`, and same-method first-`4.4s` CTE p95/max
  worsens from about `0.036/0.039m` to `0.102/0.114m`. Formal Planning is
  `61 NORMAL / 1 SPEED_FALLBACK`, has two replans (first about `+3.75s`), and
  valid gap remains about `19.99-22.94m`. Do not promote `2Hz` or continue
  scalar cutoff tuning; next align prefilter input/filter phase with consumed
  Planning target continuity, applied steer, and yaw response.
- A four-speed CARLA-direct probe now resolves the amplitude candidate exposed
  by the earlier single-speed run. Across `32/32` valid trials, static-target
  tracking falls from about `97.2%` at `4.96m/s` to `82.5%` at `19.36m/s`;
  intermediate points are about `89.9%` at `10.77m/s` and `85.8%` at
  `15.24m/s`. Fitting the observed wheel authority against the spawned
  Lincoln's native steering curve selects a `kph` speed axis with median/p95
  absolute error about `0.0071/0.0124`. This is CARLA-direct diagnostic
  evidence, not Apollo behavior evidence.
- A default-off, Baguang-only candidate maps the unchanged Apollo front-wheel
  target through `target / measured_tracking_ratio` before the unchanged
  static Lincoln inverse. Physical attribution requires the v2 calibration
  identity/hash, four speed points, per-row enabled/applied state, positive
  ratio, exact query-angle contract, and no fallback. The physical target is
  `raw * steering_sign * apollo_max_steer_angle_deg`; legacy `steer_scale` is
  not part of that formula. Defaults and Town01 remain unchanged.
- Strict pair
  `runs/phase1_online_pairs/cycle_inf_baguang_speed_compensated_steering_v13_20260718`
  is completed/evaluable/comparable with no invalid row; PlanningControl
  succeeds. Runtime evidence loads the v2 Lincoln calibration and observes
  compensation enabled/applied on all `56/56` sampled commands with `0deg`
  query-contract error p95 and no fallback. Routing, a stationary-actor fresh
  `176`-point NORMAL trajectory, and later fresh `116`-point speed-compatible
  NORMAL handover all precede formal time; Control tx before handover is zero,
  and ego/lead/gap are about `19.50/19.50m/s` and `20.005m`.
- The online A/B is negative for behavior promotion. Apollo still lane-invades
  without collision at formal `+11.10s`, versus matched v12 at `+10.75s`.
  First-`4.4s` CTE p95/max worsens from about `0.029/0.041m` to
  `0.046/0.098m`; raw steer crosses `0.25%` at about `+1.85s` instead of
  `+2.60s`; and formal replans through the event increase from `1` to `10`,
  with the first moving from about `+10.00s` to `+2.85s`. Formal Planning is
  `92 NORMAL / 1 SPEED_FALLBACK`, and v-t-gap remains valid. Speed compensation
  is therefore quarantined and not promoted; the next root-cause target is
  repeated-state Control output drift and replan-target continuity.
- A vehicle-scoped CARLA-direct steering step probe now separates actuator
  dynamics from Apollo/bridge cadence. The valid `10ms` run
  `runs/diagnostics/cycle_inf_baguang_steering_response_probe_10ms_v4_20260718`
  contains `8/8` bidirectional trials at `19.22-19.25m/s` using the same
  Lincoln calibration identity/hash as v12. Dead time is `10ms`, median/p95
  rise time is `20/30ms`, and median/p95 settling time is `30/40ms`. This is
  CARLA-direct diagnostic evidence with Apollo Control out of loop, so it is
  ineligible for Phase 1 status, Apollo capability claims, or automatic
  latency-parameter promotion.
- The probe corrects the earlier v12 response decomposition. In the pre-replan
  v12 window, bridge `commanded_steer` to CARLA `measured_steer` correlation
  peaks one `50ms` row later, while normalized measured steer and measured
  front-wheel degrees are same-row proportional (`corr=1.0`). Command-to-yaw
  peaks about three rows later (`150ms`). Median actuator dead plus rise or
  settling is `30/40ms`, with the p95 settling bound reaching one `50ms` LQR
  step; the evidence does not support enabling steering MRAC or adding a
  vehicle latency parameter.
- The same operating-point probe exposes a separate amplitude candidate:
  static-calibration targets of `3/6deg` materialize at about
  `2.475/4.896deg` near `69kph`, or `81.6-82.5%` of target. This may indicate a
  speed-dependent CARLA steering curve or a calibration-scope mismatch, but it
  is not yet a proven cause of v12's later oscillation. No `steer_scale`, LQR,
  mapping-default, guard, Town01, or pair-status change follows from this
  diagnostic cycle.
- Reprocessing the strict matched v12 online artifacts now evaluates physical
  steering in its declared nonlinear front-wheel domain instead of applying
  the legacy linear `raw * steer_scale * steer_sign` expectation. Across `58`
  sampled bridge-decode rows, Apollo raw steering maps to the declared `30deg`
  front-wheel target with `0deg` p95 error; runtime evidence also records the
  expected Lincoln calibration identity/hash, `11` steering pairs, physical
  mapping source, and no legacy fallback. The `440` mapped-to-applied rows
  retain `0` p95 error. Control attribution changes from the false
  `fail/bridge_mapping` result to `warn/dominant_breakpoint=none`; the run
  remains an evaluable `failed/lane_invasion` behavior row. This is an evidence
  attribution repair, not an Apollo behavior repair.
- A pre-replan response audit on the same v12 evidence originally grouped the
  bridge observation frame with actuator response and overestimated
  command-to-wheel delay as about `100ms`; the vehicle-scoped `10ms` probe and
  row-level decomposition above supersede that estimate. The remaining
  behavior question is closed-loop state/Control cadence, speed-dependent
  steering gain, LQR/vehicle interaction, and Planning target continuity, not
  another steering-unit change. Apollo steering MRAC remains disabled.
- Fixed-scene Control start observation no longer depends on the periodic
  bridge stats snapshot. The bridge writes
  `fixed_scene_control_handover_ack.json` immediately after the first
  successful post-gate Control publication, and the runtime hook validates it
  against the current handover marker. This closes a v9 lifecycle confounder:
  with `artifact_stats_flush_interval_s=2.0`, the ready marker preceded formal
  time by about `2.05s` while Apollo Control was already being applied.
- Strict matched pair
  `runs/phase1_online_pairs/cycle_inf_baguang_handover_ack_matched_v12_20260718`
  uses the same extended ScenarioCase, `OpenDriveMap`, and `639.68m` XODR for
  both backends. Both rows complete and are evaluable, invalid reasons are
  empty, comparison is `comparable`, and PlanningControl succeeds. The ack
  confirms exact NORMAL Planning sequence `139`; formal time follows the first
  successful publish by `0.10s`, with zero prior Control tx, ego/lead about
  `19.548/19.547m/s`, and `20.005m` bumper gap.
- The lifecycle correction materially improves but does not solve behavior.
  Over the first formal `2.1s`, raw steer stays within about
  `-0.104%..+0.232%`, lateral error below `0.0030m`, heading error below
  `0.00075rad`, and heading-rate error below `0.00443rad/s`, roughly `4-8x`
  lower than v9. Apollo still progressively lane-invades at formal `+10.75s`
  with no collision; formal Planning is `69 NORMAL / 1 SPEED_FALLBACK` and
  valid gap remains about `18.70-22.60m`. A `lon_diff=2.64706m` NORMAL replan
  at `+9.85s` amplifies an oscillation that began earlier. This remains an
  evaluable behavior loss and not an Apollo natural-driving claim.
- v10 is invalid setup evidence for an original-scenario/extended-profile map
  mismatch. v11 validates the ack timing mechanism but used different road
  assets across backend rows, so it is diagnostic only. ScenarioCase map
  ownership remains mandatory for strict pair evidence. Python compile and
  focused tests pass; the full CARLA 0.9.16 Python suite is `2408 passed`.

- The Control-attribution evidence path is now online materialized without a
  steering or behavior change. The first v8 attempt exposed that the merged
  profile contained the per-artifact override but `ApolloAdapter.prepare()`
  did not forward it into the bridge-effective config; all four Control debug
  streams therefore remained at `1/10`. The adapter now preserves the typed
  override, and pair
  `runs/phase1_online_pairs/cycle_inf_baguang_control_raw_full_capture_v9_20260718`
  records all `524/524` canonical `apollo_control_raw` messages while the
  three redundant streams remain `53/524`. Both rows are completed and
  evaluable, invalid reasons are empty, comparison is `comparable`, and
  PlanningControl succeeds. Fresh Localization/Chassis is about `18.05Hz`
  against `20.27Hz` Control; the extra raw evidence raises queue peak/maximum
  lag to `1620/6.10s`, so this remains a diagnostic capture profile rather
  than a runtime-load improvement.
- v9 independently preserves the readiness boundary. Initial-state
  materialization at sim `13.085160935s` sees Routing success, a fresh
  `176`-point non-fallback NORMAL trajectory, formal time false, and ego/lead
  below `1e-6m/s`. Formal handover at `19.085161024s` has two compatible
  NORMAL messages, a current fresh `121`-point NORMAL trajectory, ego/lead
  about `19.483/19.482m/s`, gear `4`, `20.005m` bumper gap, and zero
  pre-handover Control tx. Apollo nevertheless progressively lane-invades at
  formal `+6.70s`, about `233.49m` from road start and `0.751m` CTE, with no
  collision; this is an evaluable behavior failure, not invalid evidence.
- Full-cycle Control evidence materially narrows the lateral chain. All `57`
  formal Planning messages before the event are NORMAL and none is fallback.
  Across `103` distinct formal Control timestamps, Apollo steer spans about
  `-8.36%..+9.30%` with ten sign reversals; command-to-feedback correlation
  is strongest two applied frames later (`0.956`, about `0.10s`). LQR heading
  and lateral contributions reach about `+22.82/-13.88` and
  `+12.47/-11.17` percentage points. A NORMAL replan at `+2.10s` reports
  `lon_diff=2.50387m`; after oscillation grows, consumed trajectory sequence
  `202` shifts the Control target line about `+0.57m`, and the explicit
  `+5.50s` replan reports `lat_diff=0.507834m` while shifting it about
  `-0.78m`. This establishes Planning trajectory-replan lateral
  discontinuity as an amplifier of the existing closed-loop oscillation. It
  does not yet prove whether the first low-amplitude instability originates
  in Control dynamics, the first longitudinal replan, or their interaction;
  no scale/gain/mapping change follows. Python compile and `143` focused tests
  pass; the full CARLA 0.9.16 Python suite is `2407 passed`.
- The exact cadence-candidate repeat is now negative. Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_control_state_cadence_v4_20260718`
  remains completed/evaluable/comparable with strict NORMAL readiness, but
  Apollo progressively lane-invades at formal `+8.15s`; a same-state Control
  group changes steering by about `8.96` percentage points. Therefore the
  single v3 success does not justify promotion of the `50ms` candidate.
- Two GT-loop A/Bs isolate runtime load without changing steering mapping.
  Raising bridge cache polling from `20Hz` to `40Hz` in
  `cycle_inf_baguang_gt_source_poll_40hz_v5_20260718` is harmful: fresh
  Localization/Chassis falls to about `11.93Hz`, Control/state ratio rises to
  `1.69`, and Apollo lane-invades. Keeping bridge/Control at `20Hz` while only
  moving the growing stats snapshot to `2s` in
  `cycle_inf_baguang_control_cadence_stats_flush_2s_v6_20260718` is also not
  sufficient: GT is about `14.35Hz`, queue lag reaches `14.8s`, and the same
  real lane failure recurs. Neither candidate is a control or capability fix.
- The default-off combined low-capture pair
  `runs/phase1_online_pairs/cycle_inf_baguang_control_cadence_low_capture_v7_20260718`
  reduces redundant full Control-debug serialization while retaining all
  `432` frame-level `control_apply_trace` rows and critical-window evidence.
  Fresh GT improves to about `17.76Hz`, Control/state ratio to `1.14`, bridge
  overruns to `48`, queue peak to `707`, and maximum queue lag to `2.89s`.
  Both rows are evaluable, invalid reasons are empty, comparison is
  `comparable`, and PlanningControl succeeds. Apollo nevertheless has a
  geometry-backed progressive lane invasion at formal `+7.65s`, about
  `244.11m` from road start, with no collision. In the preceding `1.55s`,
  Apollo raw steering reverses from about `+8.7%` through `-10.3%` to
  `+11.4%`; physical mapping remains sign-consistent and mapped-to-applied
  actuation remains intact. The next blocker is Apollo lateral/reference-
  target oscillation under the still-imperfect `17.76Hz` GT feed, not a
  recurrence of steering-unit decoding. This remains diagnostic evidence,
  not natural-driving or default-promotion evidence. Focused config/dispatch
  tests are `89 passed`; the CARLA 0.9.16 Python full suite is `2405 passed`.
- A default-off Baguang diagnostic now aligns Apollo Control's timer and LQR
  discrete step with the `50ms` CARLA/GT state tick; it does not change
  steering scale/sign, physical calibration, longitudinal mapping, Planning,
  guards, scenario ownership, or Town01. In the matched baseline, Apollo
  Control ran at about `67.51Hz` against about `16.42Hz` fresh Localization/
  Chassis and emitted `362` duplicate same-state groups; `192` groups changed
  output without a new Control timestamp/Planning sequence. The worst such
  steering range was about `10.27%`.
- Online pair
  `runs/phase1_online_pairs/cycle_inf_baguang_control_state_cadence_v3_20260718`
  materializes the `50ms` candidate and is completed, evaluable, and
  `comparable`, with both backend rows successful and no invalid reasons.
  The actual gate remains `apollo_planning_ready`: Routing succeeds at sim
  `11.082263188s`; at `11.482263195s`, a fresh `111`-point NORMAL trajectory
  is present while ego/lead are effectively stationary and formal time is
  false. Formal start at `15.632263256s` has two distinct compatible NORMAL
  messages, a current fresh `116`-point NORMAL trajectory, ego/lead about
  `19.499/19.498m/s`, automatic gear `4`, about `0.32m/s^2` acceleration,
  `20.005m` bumper gap, and zero pre-handover Control tx.
- In that single online A/B, Control receive cadence is about `20.24Hz`, the
  Control/Localization count ratio falls from `4.11` to `1.32`, duplicate
  same-state groups fall from `362` to `93`, output-drift groups from `192`
  to `64`, worst same-state steering range from about `10.27%` to `5.91%`,
  and throttle/brake flips inside such groups from `2` to `0`. Apollo plays
  the complete `18s` profile with zero collision/lane invasion. This is one
  successful diagnostic sample, not a default promotion or natural-driving
  claim: formal Planning still contains `150 NORMAL / 2 SPEED_FALLBACK`,
  three large-station replans remain, and final gap grows to about `32.74m`
  with ego/lead about `15.89/19.18m/s`. Repetition and longitudinal
  attribution remain open.
- The prior `28.65s` matched-road materialization-to-handover delay was an
  accidental combined hold, not a requirement to wait continuously for a
  fresh Planning snapshot on every CARLA poll. Setup speed/gap/drivetrain/
  Control stability is now counted independently from distinct fresh,
  non-fallback, speed-compatible NORMAL Planning messages. A repeated stale
  snapshot can never open handover; fallback, a new ineligible message, or
  setup instability resets the relevant message evidence.
- Online pair
  `runs/phase1_online_pairs/cycle_inf_baguang_planning_freshness_gap_gate_20260718`
  proves the separated hold with the same extended ScenarioCase and mapping
  candidate. Actual `apollo_planning_ready` materialization at sim
  `11.423342757s` has Routing success, a fresh `111`-point NORMAL trajectory,
  formal time false, and stationary ego/lead. Both actors first move one
  CARLA tick later. Setup stability starts at `14.573342804s`; compatible
  NORMAL sequences `115/116` arrive at `15.223342813/15.323342815s`, and the
  latter opens handover with setup hold `16/4`, message hold `2/2`, a current
  fresh `116`-point NORMAL trajectory, automatic gear `4`, about
  `0.482m/s^2` acceleration, `20.005m` bumper gap, and zero pre-handover
  Control tx. Formal Control starts at `15.423342816s`. The strict
  materialization-to-handover delay is therefore about `3.90s` rather than
  `28.65s`, without accepting fallback or moving actors before readiness.
- Both dispatches/postprocess complete, both rows are evaluable with empty
  invalid reasons, and comparison is `comparable`; PlanningControl succeeds.
  Apollo is still a behavior loss, not a lifecycle success: formal Planning
  is `85 NORMAL / 2 SPEED_FALLBACK`, and a geometry-backed progressive lane
  invasion occurs about `244.90m` from road start at about `0.777m` CTE with
  actual footprint/marking intersection. This is distinct from the preceding
  quarantined road-end sensor event. The online final window did not contain
  a same-sequence `>0.25s` freshness gap, so that preservation branch remains
  directly offline-proven only. Runtime-hook tests are `43 passed`, the
  focused set is `128 passed`, and full pytest is `2401 passed`. The next
  blocker is downstream Planning/control-target/lateral attribution; this is
  not Apollo natural-driving evidence and does not justify steering tuning or
  default physical-mapping promotion.
- Latest matched-road pair
  `runs/phase1_online_pairs/cycle_inf_baguang_640m_proto_presence_physical_steering_extended_opendrive_20260718`
  online-proves the strict dynamic readiness boundary. The actual gate is
  `apollo_planning_ready`: at sim `12.322892570s`, Routing has succeeded, a
  fresh `111`-point non-fallback `NORMAL` trajectory is present, the official
  timer is false, and ego/lead are effectively stationary. Handover/formal
  start at `16.822892637s` has two compatible `NORMAL` messages, a current
  `119`-point `NORMAL` trajectory, ego/lead `19.546/19.544m/s`, gear `4`,
  about `0.076m/s^2` acceleration, a `20.005m` bumper gap, and zero Control
  transmissions before handover. Both dispatches complete, both rows are
  evaluable, invalid reasons are empty, and comparison is `comparable`;
  PlanningControl succeeds. Apollo remains an evaluable lane-invasion failure
  with zero collision. This proves lifecycle order, not behavior success or
  natural driving.
- Presence-aware protobuf capture removes fake default `(0,0)` Control points,
  zero trajectory fractions, and zero-header trajectory ages. In the new
  online sample, valid matched-point distance p95/max are about `1.95/3.32m`
  and primary input-trajectory age p95 is about `0.15s`; the previous
  `matched_point_too_large` signature disappears. A separate postprocess root
  cause was also isolated: when container map probing was empty, projection
  incorrectly fell back to `carla_town01` instead of the run-local aligned
  `OpenDriveMap`. Correct-map re-projection of the unchanged online sample is
  `80/80` valid with static route lateral p95 about `0.00017m`, but runtime ego
  lateral p95 remains about `0.296m`. The evidence pipeline is corrected;
  lateral behavior is not. Proto/runtime targeted tests are `275 passed`, map/
  postprocess targeted tests are `132 passed`, and full pytest is
  `2399 passed`.
- Integrated repeat
  `runs/phase1_online_pairs/cycle_inf_baguang_640m_correct_map_integrated_20260718`
  now proves the corrected map selection in the normal finalized postprocess
  path. Its `OpenDriveMap` HDMap projection is claim-grade `pass` with `80/80`
  valid rows, about `633.48m` coverage, static route lateral/heading p95 about
  `0.000173m/7.08e-7rad`, and runtime ego lateral p95 about `0.00639m`.
  Raw-to-mapped and mapped-to-applied steering checks pass, no lateral guard
  intervenes, and maximum absolute route CTE is about `0.00646m`.
- The same repeat retains the raw CARLA lane-invasion event about `637.08m`
  from road start, but the ScenarioCase-bound extended XODR contract shows
  only `0.000077m` CTE, about `0.906m` footprint clearance, no marking
  intersection, and a CARLA `Broken` versus XODR `Solid` type mismatch. The
  Apollo row remains `failed/evaluable` with no invalid reason, while the
  event is not backend-loss or hard-gate eligible; comparison is
  `partially_evaluable/lane_event_contract_blocks_hard_gate`. The
  postprocessor now resolves the bound XODR for noncanonical Baguang runtime
  map names instead of skipping the contract. Related tests are `115 passed`
  and full pytest under the CARLA 0.9.16 Python environment is `2400 passed`.
  This is an evidence-classification correction, not an Apollo behavior pass.
- The next runtime blocker is the setup spatial budget, not steering:
  materialization-to-handover takes about `28.65s`, followed by about `3.90s`
  to formal Control, so the authored approximately `70kph` state consumes
  nearly the full `639.68m` road before formal behavior. The next cycle must
  align drivetrain readiness, compatible NORMAL sequences, exact-match retry
  timing, actor state, and position without accepting fallback, moving actors
  earlier, extending the diagnostic road again, or changing steering.

2026-07-17 targeted online evidence:

- The exact Planning-to-Control startup gate no longer depends on two
  independent Cyber readers observing the same sequence in the same callback
  order. The bridge now retains only original Apollo Control messages whose
  declared Planning input sequence has not yet reached the bridge Planning
  reader, and re-evaluates the unchanged exact/non-fallback gate when that
  exact sequence arrives. Pending commands are sequence-keyed, bounded, and
  discarded after `0.5s`; no command, trajectory type, or control value is
  synthesized. In
  `runs/phase1_online_pairs/cycle_inf_baguang_640m_exact_match_retry_physical_steering_20260717`,
  Routing succeeds at sim `10.864s`, the first `111`-point NORMAL trajectory
  arrives at `11.114s`, and the readiness materialization at `11.214s` still
  observes ego/lead effectively stationary. Drivetrain/handover readiness is
  written at `14.814s`; the same timestamp opens the publish gate on exact
  NORMAL `seq=117` through a `0.05s`-old retry, and formal time begins at
  `15.014s`. This removes the preceding sample's approximately `4.25s`
  marker-to-publish wait without accepting `N-1`, fallback, UNKNOWN, or stale
  Control. Runtime counters record `35` exact retries before/at handover, one
  retry that opens publication, and zero expired pending commands. Both rows
  remain completed/evaluable/comparable with zero invalid rows;
  PlanningControl succeeds. Apollo remains an evaluable lane-invasion failure
  with no collision: a geometry-backed solid-solid marking intersection
  occurs at formal `+10.20s`, with mapped-to-applied steering error zero.
  Formal Planning is `88 NORMAL / 3 SPEED_FALLBACK`, so the lifecycle change
  is online-proven but lateral behavior and non-uniform Planning health remain
  unresolved. Valid bumper-gap rows are `205`, starting at `20.008m`, with
  minimum `18.933m`, maximum `22.019m`, and final `19.400m`. Targeted tests are
  `94 passed`; the full suite is `2391 passed`. This is neither a behavior
  pass nor an Apollo natural-driving claim.
- A vehicle/profile-scoped physical-steering repeat of the latest `639.68m`
  drivetrain-ready candidate is recorded in
  `runs/phase1_online_pairs/cycle_inf_baguang_640m_drivetrain_ready_physical_steering_20260717`.
  The candidate changes only active steering/feedback mapping: Apollo's
  full-scale steering percentage is converted through the pinned Lincoln MKZ
  front-wheel calibration before CARLA actuation; longitudinal mapping and
  every guard/assist remain off, `steer_scale=1`, `steer_sign=-1`, and the
  mapping remains default-off. The observed runtime loaded the expected
  calibration identity and hash. Routing succeeds at sim `11.097s`; the
  readiness snapshot at `11.447s` has a fresh `111`-point NORMAL trajectory,
  while ego/lead are effectively stationary. Authored speed begins only
  after that gate, and drivetrain handover is ready at `15.497s` with both
  actors in automatic gear `4`, about `0.257m/s^2` acceleration, and a
  `20.005m` bumper gap. However, the exact non-fallback Planning-to-Control
  publish gate does not first open until `19.747s` (`seq=156`), and formal
  time starts at `19.947s`; this approximately `4.25s` marker-to-publish delay
  is retained as a diagnostic rather than relaxed. Both rows are
  completed/evaluable/comparable with zero invalid rows; PlanningControl
  succeeds, while Apollo is an evaluable `failed/lane_invasion` row with no
  collision. The geometry-backed lane contract confirms a progressive real
  marking crossing at formal `+7.45s`, with mapped-to-applied steering error
  zero. This is later than the legacy sample's `+3.25s`, but it is not a full
  repair: formal Planning contains `55 NORMAL / 5 SPEED_FALLBACK`, and the
  event remains. Valid bumper-gap rows start at `19.999m`, reach a minimum
  and final value of `19.349m`, and a maximum of `21.604m`. Targeted tests are
  `65 passed`; the full suite is `2389 passed`. This single repeat proves the
  mapping loaded and the failure moved, not that mapping alone caused the
  timing difference or that Apollo natural driving passed.
- The existing matched Baguang road candidate already provides a `639.68m`
  CARLA OpenDRIVE world and a separately densified Apollo `OpenDriveMap`
  HDMap. A new default-off composition with the native-state drivetrain-ready
  gate was exercised in
  `runs/phase1_online_pairs/cycle_inf_baguang_640m_drivetrain_ready_20260717`.
  Routing and a fresh `111`-point NORMAL trajectory precede the one-shot
  authored-speed materialization at sim `11.514s`; handover occurs at
  `15.564s` only after both vehicles are in automatic gear `4` with measured
  longitudinal acceleration about `0.257m/s^2`, and Apollo Control/formal time
  begins at `15.914s`. The pair is completed/evaluable/comparable with no
  invalid row; PlanningControl succeeds. Setup Planning is `80 UNKNOWN / 39
  NORMAL / 4 SPEED_FALLBACK`, and all `29` formal messages through formal
  `+3.25s` are NORMAL. Thus the short-map fallbacks at approximately formal
  `+0.85s/+2.95s` do not recur in the matched window. Apollo then crosses a
  Broken marking under the unchanged legacy steering mapping and is an
  evaluable `failed/lane_invasion` row with zero collision. This short formal
  window does not by itself prove uniform 18-second Planning health, although
  the earlier matched-map physical-steering sample independently recorded
  `177/177` formal NORMAL messages over the complete actor profile. The
  inherited but inactive terminal stop hold is explicitly disabled in the new
  candidate; no steer scale, Planning constraint, physical-mapping default,
  ScenarioCase timing, or Town01 profile changes. Related targeted tests are
  `64 passed` and the full suite is `2388 passed`.
- The default-off native-state startup candidate now removes the long
  scene-owned handover interval without weakening the gate. In
  `cycle_inf_baguang_dynamic_native_state_handover_20260717`, Routing and a
  `111`-point NORMAL trajectory are present while ego/lead remain `0/0m/s`.
  After the authored speed is materialized, `seq=82` is a rejected
  `SPEED_FALLBACK`; `seq=83/84` are two current-speed-compatible NORMAL
  trajectories. Handover readiness occurs after `0.40s`, and Apollo
  control/formal time starts after `0.60s`, versus `4.30/4.45s` in the
  minimal-input sample. The input boundary uses CARLA physical acceleration,
  a marker-bounded one-frame authored-transition rule, sim-time Localization
  with the existing mock Cyber clock, and Apollo's native current-state
  caution Prediction path. It does not change Planning constraints, steering,
  mapping, or the ScenarioCase. Both rows are evaluable, invalid rows are
  empty, comparison is `comparable`, PlanningControl succeeds, and Apollo's
  later lane invasion remains a valid behavior failure rather than a setup
  failure or natural-driving pass.
- Dynamic `lead_decel_accel` ownership/readiness A/Bs now separate three
  startup choices. The matched baseline
  `cycle_inf_baguang_dynamic_ownership_baseline_20260717` scene-controls ego
  and lead for about `7.70s` and starts formal time about `8.05s` after the
  first NORMAL trajectory. The default-off minimal authored-speed candidate
  `cycle_inf_baguang_dynamic_minimal_compatible_handover_20260717` assigns the
  canonical `19.44m/s` initial condition only after Routing and a `111`-point
  NORMAL trajectory, then requires two distinct current-speed-compatible
  NORMAL messages. Its pair is completed/evaluable/comparable with no invalid
  row, and formal time starts `4.45s` after assignment; PlanningControl
  succeeds while Apollo has a valid lane-invasion behavior failure. This is a
  setup-latency improvement, not an Apollo behavior pass.
- Giving Apollo ego immediately after the first NORMAL trajectory is not a
  valid replacement for this canonical case. In
  `cycle_inf_baguang_dynamic_apollo_owns_accel_setup_20260717`, Apollo owns ego
  after `0.10s`, but reaches only `11.96m/s` and incurs lane invasion before
  the authored `70kph` initial condition is ready. Formal scenario time never
  starts; the Apollo row is correctly `invalid/setup_failed`, not a backend
  loss, and the comparison is only partially evaluable. The repository also
  continues to reject the unsafe combination of one-shot speed assignment and
  immediate `planning_ready` handover.
- The isolated marker-bound Localization transition A/B
  `cycle_inf_baguang_dynamic_minimal_transition_20260717` proves that the
  one-shot assignment creates a `-379.71m/s^2` finite-difference sample and
  zeros exactly that setup-only sample. It does not improve readiness:
  assignment-to-formal time is `4.60s` versus `4.45s`, and fallback remains.
  The single GT acceleration sample is therefore not sufficient by itself.
  The native-state candidate above shows that the coherent startup input
  boundary prevents the `56.96m/s` fallback initial-speed artifact and makes
  compatible NORMAL Planning available within two messages. Relative
  contribution of its time/Prediction/acceleration inputs remains a promotion
  question, not a reason to relax fallback rejection. Related focused tests
  for the new profile are `15 passed`; the full suite is `2385 passed`.
- Native-8m repeat and runtime-evidence hardening
  `runs/phase1_online_pairs/cycle_inf_baguang_followstop_static_native8_stats_snapshot_fix_20260717`
  repeats the Apollo stop result at `8.163m` with zero collision/lane
  invasion. The strict gate accepts a stationary ego/lead and a fresh
  `111`-point NORMAL trajectory; Apollo owns ego about `0.15s` before formal
  scenario time. Apollo is evaluable `success`, PlanningControl remains an
  evaluable `failed/unsafe_gap` at `7.721/7.776m`, invalid rows are empty, and
  comparison is `comparable`.
- Default-off stationary-start brake-domain A/B
  `runs/phase1_online_pairs/cycle_inf_baguang_followstop_static_brake_scale_v38_20260719`
  is completed/evaluable/comparable and preserves the strict ownership
  boundary. Apollo receives ownership with ego/lead at zero speed only after
  Routing plus a fresh `111`-point NORMAL trajectory; pre-handover Control tx
  is zero. Full Planning is `1 UNKNOWN / 737 NORMAL`, with no fallback or
  watchdog actuation. Apollo reaches a stop with zero collision/lane invasion,
  but minimum/final valid bumper gaps are `7.86812/7.86826m`, so the authored
  `8.0m` contract correctly emits evaluable `failed/unsafe_gap`. This rejects
  the constant `0.38` scale as a follow-stop promotion candidate; it is not a
  setup failure and zero collision is not a substitute for the stop-gap gate.
- The preceding same-config repeat exposed an independent bridge lifecycle
  defect: periodic stats serialization raced nested callback mutation and
  exited with `RuntimeError: dictionary changed size during iteration`.
  Subsequent watchdog braking was platform safety behavior, not Apollo
  follow-stop success. Stats writing now serializes a recursively detached
  container snapshot, and `phase1_status` classifies an unhandled bridge
  traceback as `invalid/bridge_runtime_failed`, never a backend loss. In the
  fix run the bridge has no traceback or publish error and spans the complete
  scene with `1279` localization/chassis publishes, `887` nonempty target
  obstacle messages, `886` Planning messages (`805` nonempty), and Control
  through the terminal stopped window.
- The online GT stream also exposed an evidence-classification mismatch, not
  a publish loss. Its 20Hz probe rows contain `887 source_fresh_published`
  rows and `392 source_frame_not_fresh` cadence skips for the 10Hz obstacle
  source. All `887` actual publishes contain an obstacle and the target actor
  linkage passes. The obstacle-GT analyzer now distinguishes those explicit
  cadence skips from unexplained zero-count rows; reprocessing changes the
  false `apollo_obstacle_gt_contract_failed` invalid result to
  `success/evaluable` without changing runtime behavior. Unexplained drops
  remain failures. Related tests are `89 passed`; the full suite is
  `2380 passed`.
- The native-8m Planning anomaly is not closed: the fix run repeats one formal
  `SPEED_FALLBACK` at `seq=291 / 69 points` while route segments and four
  reference lines remain present. The gate never accepts fallback, but this
  repeated single-frame event keeps the candidate default-off and prevents a
  profile-promotion claim. None of this is Apollo natural-driving evidence.
- Source-log alignment localizes that formal fallback to a forced replan whose
  Piecewise Jerk initial acceleration equality is `3.216676m/s^2`, while the
  active Apollo vehicle model bounds acceleration at `2.0m/s^2`. The same QP
  cannot satisfy both constraints; the normal and relaxed-velocity attempts
  are primal-infeasible before constant-deceleration fallback. Route, reference
  line, and target-obstacle ST bounds remain materialized in that frame.
- Default-off vehicle-contract A/B
  `runs/phase1_online_pairs/cycle_inf_baguang_followstop_static_accel4_ab_retry_20260717`
  changes only this follow-stop candidate's
  `vehicle_param.max_acceleration` to `4.0m/s^2`. Both dispatches and
  postprocess chains complete; both rows are evaluable, invalid rows are
  empty, and comparison is `comparable`. Apollo is conservatively
  `degraded/evaluable` because some gap rows use a degraded method, although
  its minimum/final valid gap is `8.201m`; PlanningControl is the unchanged
  evaluable `failed/unsafe_gap` row at `7.721/7.776m`.
- The A/B captures `81 UNKNOWN` empty startup rows followed by `658 NORMAL`
  rows. After the first NORMAL trajectory, non-NORMAL count is zero; three
  later longitudinal-distance replans (`seq=266/286/322`) all publish NORMAL,
  versus the repeatable formal `SPEED_FALLBACK` with the 2.0 model. Routing
  succeeds at sim `102.273789s`, first `111`-point NORMAL Planning is at
  `102.573789s`, stationary handover is at `102.673789s`, Apollo Control starts
  at `102.823789s`, and formal scenario time starts at `103.073789s`.
  Initial-speed materialization remains disabled and the scene never drives
  ego.
- The candidate/config boundary has Python compile and `11` focused tests;
  the full shared-tree suite is `2381 passed`.
- Same-config repeat
  `runs/phase1_online_pairs/cycle_inf_baguang_followstop_static_accel4_repeat_20260717`
  also completes both runtimes and all postprocess/cleanup stages. Its
  Planning stream is `82 UNKNOWN / 663 NORMAL`; all `663` messages from the
  first NORMAL onward and all `657` formal-window messages are NORMAL. Later
  forced replans at `seq=268/289/329` also publish NORMAL. Routing succeeds at
  `101.668523s`, first `111`-point NORMAL Planning is at `101.968524s`,
  stationary handover is at `102.068524s`, Apollo Control starts at
  `102.268524s`, and formal time starts at `102.468524s`. Pre-handover Control
  tx remains zero and initial-speed materialization remains disabled.
- The repeat is again `degraded/evaluable` only because `7/1624` v-t-gap rows
  use the lateral-degraded projection method; its non-degraded minimum/final
  gap is `8.191m`, collision/lane-invasion counts are zero, and comparison is
  `comparable`. PlanningControl remains the same evaluable
  `failed/unsafe_gap` result at `7.721/7.776m`; invalid rows remain empty.
- The two valid 4.0 samples are repeatability evidence, not promotion proof.
  Source/log alignment shows that the first sample's residual Piecewise Jerk
  first-attempt failures are local-speed-bound conflicts recovered by Apollo's
  native relaxed-velocity retry; they are not the baseline's unrecovered
  initial-acceleration-bound failure. Measured positive acceleration still has
  p99/maximum transients around `4.66/10.65m/s^2`, so raising the vehicle bound
  further would overstate safe planning capability. Vehicle-model provenance
  and Town01/remaining-Baguang no-regression remain required before any
  profile-scoped promotion. The preceding `accel4_ab` attempt is setup-invalid
  because a diagnostic `claim_profile: false` accidentally selected the
  unwired typed-v0 runner; it never entered Apollo and is not a backend loss.
- Native Apollo static-obstacle stop-distance A/B
  `runs/phase1_online_pairs/cycle_inf_baguang_followstop_static_native8_retry_20260717`
  localizes the repeated `6.229-6.336m` Apollo gap to the compiled Planning
  default `min_stop_distance_obstacle=6.0`. The default-off follow-stop-only
  candidate now materializes `--min_stop_distance_obstacle=8.000`; default
  Baguang and Town01 profiles remain unchanged. Captured Planning emits the
  target actor's stop fence at `distance_s=-8.0`, and Apollo stops with a valid
  non-degraded minimum/final bumper gap of `8.212m` over a `714`-row, `35.65s`
  stopped window. Apollo is therefore evaluable Phase 1 `success` for this
  pair; PlanningControl remains an evaluable `failed/unsafe_gap` at
  `7.721/7.776m`, invalid rows are empty, and comparison is `comparable`.
- The A/B preserves the strict ownership boundary: Routing succeeds at sim
  `104.892939s`, first nonempty Planning arrives at `105.192939s`, handover at
  `105.292939s` accepts `seq=82 / 111 points / NORMAL` with ego/lead both
  stationary, Apollo Control starts at `105.442939s`, and formal scenario time
  starts at `105.692939s`. Control tx is zero before handover and three after;
  initial-speed materialization, terminal hold, and all lateral guards remain
  disabled. Python compile, `71` related tests, and the full suite
  (`2377 passed`) support the shared overlay change.
- This is one default-off scenario/profile result. Persisted Planning includes
  `499 NORMAL / 1 SPEED_FALLBACK / 82 UNKNOWN`; the gate did not accept the
  fallback, but repeat and no-regression evidence remain necessary. The pair
  has no `natural_driving_report.json` and is not Apollo natural-driving or
  global mapping-promotion evidence.
- Zero-speed static follow-stop pair
  `runs/phase1_online_pairs/cycle_inf_baguang_followstop_static_planning_ready_portable_steering_v2_20260716`
  proves that the strict ownership boundary does not require the scene player
  to accelerate ego. The real RoutingResponse is at sim `101.686701s`, the
  first nonempty Planning output is `seq=82 / 111 points / NORMAL` at
  `101.936701s`, and ego ownership transfers at `102.036701s`. Apollo's first
  Control reaches the boundary at `102.186701s`, only `0.15s` after handover;
  official scenario time begins at `102.436702s`, `0.25s` after Apollo Control
  starts. Control tx count is zero before handover and three afterward.
- Runtime materializes `start_gate=apollo_planning_ready`, requires Routing,
  at least two trajectory points, freshness, and non-fallback Planning, and
  keeps initial-speed materialization disabled. The seven setup trace rows
  control only `lead_vehicle` with zero throttle/brake/steer; there are zero
  scene-owned ego-control rows. The apparent initial `2.45m/s` speed norm is a
  vertical CARLA spawn-settling transient (`z: 0.1815 -> -0.0051m`): planar
  ego displacement and maximum planar step speed before handover are both
  `0`, and measured speed is `0m/s` at handover.
- The first online attempt without the zero-value fix is invalid
  `setup_failed/scene_preroll_target_speed_missing`; it is not an Apollo
  behavior loss. The root cause was a truthiness check that treated the
  authored static target speed `0.0m/s` as missing. The accepted v2 pair has
  both dispatches completed, both rows evaluable, comparison `comparable`,
  and no invalid row. Complete Apollo Planning is `431 NORMAL / 81 UNKNOWN`
  with zero nonempty fallback; all `424` formal messages are nonempty NORMAL.
  Collision and lane-invasion counts are zero, the stopped-lead actor contract
  passes, and all `852` v-t-gap rows use valid non-degraded bumper projection.
- The old Phase 1 `success` labels for this pair were incorrect because the
  classifier treated only negative gap as unsafe and did not consume the
  authored `min_stop_gap_m: 8.0`. The classifier now evaluates the final
  contiguous stopped window using only valid, non-degraded bumper-gap methods.
  Reprocessing the unchanged online artifacts classifies both rows as
  evaluable `failed/unsafe_gap` behavior results, with empty invalid reasons;
  comparison remains `comparable`. Apollo's stopped tail is `20` rows over
  `0.95s`, with minimum/final gap `6.229m`. PlanningControl's stopped tail is
  `1407` rows over `70.3s`, with minimum gap `7.721m` and final gap `7.776m`.
  Both violate the authored `8.0m` threshold. This is a deterministic
  classification correction of existing evidence, not a new online runtime
  result. The sample proves strict readiness, prompt Apollo ownership,
  portable steering execution, and comparison materialization, but neither a
  follow-stop behavior pass nor a `natural_driving_report.json`-backed Apollo
  natural-driving claim.
- The static compatibility path previously overrode an explicitly selected
  dynamic sidecar with the legacy static profile. Explicit repository Baguang
  sidecar selection now takes precedence while default static dispatch remains
  unchanged. The zero-speed check uses presence rather than truthiness.
  Python compile, `91` focused runtime/launch/pair tests, and the full suite
  (`2372 passed`) cover these shared changes. The candidate remains
  default-off; physical mapping is not promoted and `steer_scale` is unchanged.
- The stopped-gap classifier correction is covered by pass, fail,
  moving-final-state, and degraded-gap regressions. The focused
  status/comparison/pair-runner set is `92 passed`; the real pair statuses and
  comparison were regenerated in place while preserving the raw online
  artifacts. The current full suite is `2376 passed`.

2026-07-16 targeted online evidence:

- Portable Baguang steering-calibration pair
  `runs/phase1_online_pairs/cycle_inf_baguang_cut_in_portable_steering_calibration_20260716`
  closes the local/ignored calibration-file dependency for the quarantined
  physical-steering candidate. Both Baguang physical branches now point to
  `configs/calibration/vehicles/vehicle.lincoln.mkz_2020/steering_front_wheel_v1.json`.
  The compact file carries the exact 11-point front-wheel-angle inverse table,
  vehicle identity, and source-library provenance; it intentionally contains
  no longitudinal tables because the candidate keeps
  `map_longitudinal/map_throttle/map_brake=false`.
- Runtime stats prove that the bridge loaded that repository file with
  calibration id
  `carla_0_9_16__vehicle.lincoln.mkz_2020__steering_front_wheel_v1`, vehicle
  `vehicle.lincoln.mkz_2020`, and SHA-256
  `5951ecc556a2243d63e7ad39b2ca4de1f5d7699a368c69ca5626e3b0c56cefae`.
  A dense `-70deg..+70deg` differential over `140001` inputs reports zero
  output difference from the original full local calibration. The matched
  runtime effective configs differ only in run-local paths/ids and the
  calibration path. Existing `control_health` now consumes this loaded runtime
  identity and records the same id/hash/vehicle plus `steer_scale=1.0` and
  `steering_sign=-1.0`, instead of the false `calibration_profile_missing`
  warning. Python compile, `130` targeted
  config/mapping/bridge/launch/control-health tests, and the full suite
  (`2369 passed`) support the shared loader/status/analyzer change.
- Strict readiness and ownership remain unchanged online. Routing count `1`
  and a fresh `111`-point NORMAL trajectory open
  `apollo_planning_ready` with pre-materialization ego/target speeds `0/0m/s`;
  handover follows after `0.40s`, official Apollo/scenario time after `0.55s`,
  and Control tx is zero before the marker. Both pair rows are
  completed/evaluable Phase 1 `success`, comparison is `comparable`, invalid
  rows and safety-event counts are zero, the actor contract passes with a
  `3.481m` lateral shift and no teleport, and all `578` v-t-gap rows are valid
  (`1.855m` minimum, `18.816m` final gap). Formal Planning still contains
  `16` SPEED_FALLBACK rows, so the candidate remains quarantined; this is not
  default-mapping promotion or Apollo natural-driving evidence. Town01 online
  no-regression and the other affected Baguang P0 cases remain required before
  any vehicle/profile-scoped promotion.
- Short-handover candidate
  `runs/phase1_online_pairs/cycle_inf_baguang_early_handover_tolerance_20260716`
  now has two same-session repeats:
  `cycle_inf_baguang_early_handover_tolerance_repeat_20260716` and
  `cycle_inf_baguang_overlay_restore_lifecycle_20260716`. Across the three
  `lead_decel_accel` samples, the strict `apollo_planning_ready` gate opens
  with both actors stationary after Routing success and a `111`-point NORMAL
  trajectory. The atomic setup marker follows after `0.50-0.55s`, and formal
  Apollo control follows after `0.60-0.70s`, versus about `3.55s / 3.65s` in
  the preceding one-shot initial-state-transition sample. The formal
  lead-speed phase starts in the same frame as official Apollo ownership, so
  setup control does not consume the scored `18s` phase.
- The candidate changes only the explicit setup speed tolerance to `1.0m/s`.
  At handover both actors are about `18.901m/s`, only `0.539m/s` (`2.77%`)
  below the authored `19.44m/s`, with about `20.007m` bumper gap. In the latest
  lifecycle repeat, handover is about `18.892m/s`, `20.007m`, and `0.432m/s`
  from the consumed trajectory's current speed. A setup SPEED_FALLBACK resets
  compatibility; only two later distinct compatible NORMAL trajectories open
  handover. The latest complete Planning stream is `185 NORMAL / 1
  SPEED_FALLBACK / 81 UNKNOWN`; no Apollo control reaches CARLA before
  handover, and all repeated pairs complete as comparable/evaluable Phase 1
  `success` with zero collision/lane invasion and no invalid reason. Python
  compile, `31` overlay tests, and the full suite (`2364 passed`) support the
  current code state.
- Cross-scenario cut-in repeat
  `runs/phase1_online_pairs/cycle_inf_baguang_cut_in_role_preroll_repeat_20260716`
  closes two runtime portability defects hidden by the equal-speed lead case.
  Setup control/readiness now preserves the authored ego/target speeds
  `11.11/9.72m/s` and honors the cut-in spawn's default
  `center_to_center=10m` contract while recording bumper gap separately. A red
  regression also caught and corrected a handover artifact that mislabeled
  the center gap as `bumper_gap_m`. Python compile, `40` runtime tests, `71`
  compiler/schema/dispatch tests, and the full suite (`2365 passed`) support
  the corrected shared runtime.
- In the clean repeat, Routing response at sim `46.365763s` and the first
  `111`-point NORMAL trajectory at `46.565763s` precede the strict
  `apollo_planning_ready` gate at `46.715763s`. Both actors are observed at
  `0m/s` immediately before gate-time materialization; pre-gate planar
  displacement is `0m` for ego and about `0.00017m` for the target. Freshly
  spawned actors do show an earlier vertical-settling speed-norm transient,
  which is recorded rather than relabeled as perfect zero-velocity startup.
  The handover marker follows at `47.065763s` with about `10.813/9.376m/s`, a
  `9.522m` center gap, and a `4.630m` bumper gap. Official Apollo ownership
  and scenario time start together at `47.165763s`, only `0.45s` after the
  gate; no Apollo control transmission precedes the handover marker.
- Both repeat rows are completed/evaluable Phase 1 `success`, comparison is
  `comparable/apollo_vs_planning_control_evaluable`, invalid reasons are
  empty, and collision/lane-invasion counts are zero. The cut-in actor
  contract passes with a `3.481m` lateral shift and no teleport; all `579`
  Apollo v-t-gap rows are valid, with bumper gap about `4.494m` initially,
  `2.131m` minimum, and `19.215m` finally. The preceding same-profile sample
  independently reproduces the behavior result with a `0.60s` gate-to-formal
  delay, but its pre-fix handover gap label is not authoritative.
- This does not promote the profile or prove Apollo natural driving. Complete
  repeat Planning is `270 NORMAL / 15 SPEED_FALLBACK / 82 UNKNOWN`; every
  fallback is in formal time from about `+1.20s` to `+3.40s`, overlapping the
  minimum-gap interval near `+2.15s`. The strict gate accepts only NORMAL, but
  the formal cut-in fallback cluster is a remaining Planning diagnostic
  boundary.
- The fallback task chain is now attributed. All `15` formal baseline rows
  keep `4` reference lines, `1` Routing road, `1` segment, `LANE_FOLLOW`, and
  source-fresh GT, while each lacks a successful `PIECEWISE_JERK_SPEED` graph.
  Apollo 10 source records that graph only after successful optimization;
  failure clears speed data and the native fallback task sets
  `SPEED_FALLBACK`. The cluster follows the target's lateral ingress and
  minimum-gap interval; later FOLLOW boundaries begin at `t=0` with negative
  longitudinal clearance, so the current nonzero ego state cannot immediately
  satisfy the ST speed bounds.
- Matched negative A/B
  `runs/phase1_online_pairs/cycle_inf_baguang_cut_in_current_state_prediction_ab_20260716`
  switches only the relevant Apollo Prediction semantics to native
  `COST_EVALUATOR + MOVE_SEQUENCE_PREDICTOR` under a default-off profile.
  Apollo logs `243` COST target rows; selected `+1s/+3s` target speeds stay
  about `9.52-9.69m/s` with a `0.10m/s` maximum adjacent change, eliminating
  the Jointly baseline's `7.47m/s` jump. Formal fallback nevertheless changes
  from `15` to `17` rows (`259 NORMAL / 17 SPEED_FALLBACK`) rather than
  disappearing. The smoother Prediction is therefore not a fix and the
  candidate is not promoted.
- The A/B retains the strict stationary boundary: Routing and a fresh
  `111`-point NORMAL trajectory precede the gate, both pre-materialization
  speeds are `0m/s`, and official Apollo/scenario time starts `0.55s` after
  the gate with zero control transmissions before handover. Both backend rows
  are completed/evaluable Phase 1 `success`, comparison is
  `comparable/apollo_vs_planning_control_evaluable`, invalid and safety-event
  counts are zero, actor lateral shift is `3.481m` with no teleport, and all
  `578` v-t-gap rows pass with a `3.590m` minimum bumper gap. Related targeted
  tests are `94 passed`; the full suite is `2366 passed`.
- Matched steering-domain A/B
  `runs/phase1_online_pairs/cycle_inf_baguang_cut_in_legacy_steering_ab_20260716`
  confirms that the strict short handover does not depend on physical mapping,
  but the accepted cut-in behavior does. The online resolved backend configs
  differ only at `actuator_mapping_mode: physical -> legacy`; `steer_scale=1.0`,
  longitudinal legacy mapping, source-fresh/coherent GT, strict fallback
  rejection, target-ready handover, and disabled guards are unchanged.
  Routing count `1` plus a fresh `111`-point NORMAL trajectory opens stationary
  materialization at sim `46.885958s`; handover follows after `0.55s` on
  `seq=88 NORMAL / 116 points`, and official Apollo/scenario time follows after
  `0.65s`. Control tx remains zero before handover.
- Both dispatches and postprocess complete, invalid rows remain empty, and the
  comparison is `comparable/apollo_vs_planning_control_evaluable`.
  PlanningControl succeeds; Apollo is a valid behavior
  `failed/lane_invasion` row rather than a setup failure. It crosses a Broken
  marking at formal `+19.80s`, with zero collision, CTE about `-0.445m`,
  heading error about `-0.228rad`, and a confirmed footprint intersection.
  The actor contract still passes with about `3.528m` lateral shift and no
  teleport.
- Over the matched first `19.8s`, legacy CTE p95/max is about
  `0.265/0.560m` versus `0.085/0.086m` for the clean physical repeat;
  heading-error p95/max is about `0.152/0.230rad` versus
  `0.0054/0.0101rad`. Legacy first exceeds `0.1m` CTE at `+8.15s` and `0.1`
  raw steer at `+12.85s`; its four PATH_FALLBACK rows begin later at
  `+13.80s`. Mapped-to-applied steering remains consistent through the
  departure. The two same-profile physical runs both finish with zero safety
  events, supporting a bidirectional steering-domain dependency rather than a
  readiness or dropped-actuation explanation.
- This A/B does not promote physical mapping. The new legacy profile is
  default-off, the active five-P0 matrix and Town01 profiles are unchanged,
  `109` related tests pass, and the full suite is `2367 passed`. Portable
  calibration identity plus Baguang/Town01 no-regression remain required;
  neither candidate supports an Apollo natural-driving claim.
- Lowering Apollo's longitudinal replan threshold to `0.5m` is negative A/B
  evidence: it triggers replanning by about `0.55s` but does not shorten the
  roughly `3.55s` handover, and the Apollo row later has an evaluable lane
  invasion. That candidate must not be promoted. The tolerance result also
  remains quarantined pending cross-scenario regression. It is not
  Apollo `0 -> 70kph` evidence, and the final Apollo gap of about `41.57m`
  prevents a representative-following or natural-driving claim.
- The latest repeat also online-verifies Apollo managed-config cleanup. Before
  the fix, normal `stop()` restored only the Control binary overlay and left
  Prediction, Planning, Control, flagfile, and vehicle-parameter config links
  pointed into `conf_overlay`. `CyberRTBackend` now restores only symlinks that
  resolve inside its managed overlay root both at stop and defensively at the
  next start. The lifecycle pair restores seven backed-up files, removes one
  originally absent target, records zero restore errors, and leaves every
  post-run file hash or missing state identical to the pre-run baseline.
- Native current-state Prediction A/B
  `runs/phase1_online_pairs/cycle_inf_baguang_initial_state_gate_current_state_prediction_ab_20260716`
  proves that Apollo's own `COST_EVALUATOR + MOVE_SEQUENCE_PREDICTOR` path can
  remove the learned-evaluator fresh-history speed collapse without fabricating
  target history. The first visible lead is measured at `18.986m/s`, and its
  first selected Prediction trajectory remains `18.986m/s` at `+1s`, versus
  about `0.132m/s` for Jointly and `0.027m/s` for VectorNet. The managed
  overlay is default-off and rewrites only `VEHICLE/ON_LANE/CAUTION`; Apollo
  logs `218 COST_EVALUATOR` target rows. Python compile, `81` related tests,
  and the full suite (`2349 passed`) succeed.
- This is a partial diagnostic result, not a promoted fix. The strict gate
  passes at sim `11.503s` with Routing success, a `111`-point NORMAL
  trajectory, no fallback, and ego/lead speeds below `2e-6m/s`. Target
  activation follows at `11.553s`, but formal Apollo ownership does not begin
  until `15.553s`: the setup pre-roll still controls both ego and lead for
  about `4.00s / 69.60m`. This is shorter than the matched Jointly sample
  (`4.45s / 77.44m`) and VectorNet sample (`4.80s / 84.29m`), but it confirms
  the user's concern that the present compatibility handover remains late.
  The current runtime must therefore not be described as Apollo owning ego
  from process start.
- The Planning transition is not clean enough for promotion. The first
  post-activation message is a 62-point SPEED_FALLBACK with about
  `56.96m/s` current and `52.96m/s` `+1s` speed after a backward-time
  trajectory-stitcher replan; Piecewise Jerk fallback is primal-infeasible.
  The complete stream is `209 NORMAL / 7 SPEED_FALLBACK / 82 UNKNOWN`, and
  formal time still contains `172 NORMAL / 2 SPEED_FALLBACK`. Nevertheless,
  both rows are completed/evaluable/comparable, invalid count is zero, both
  Phase 1 statuses are `success`, collision/lane-invasion counts are zero, and
  all `361` Apollo v-t-gap rows are valid (`18.25m` minimum, `34.41m` final).
  This closes learned Prediction history as the sole blocker and moves the
  next investigation to gate-time velocity discontinuity, trajectory
  stitching, and Piecewise Jerk constraints; the gate remains strict and no
  natural-driving claim is allowed.
- Native Prediction evaluator A/B
  `runs/phase1_online_pairs/cycle_inf_baguang_initial_state_gate_noninteractive_prediction_ab_v2_20260716`
  falsifies interactive-tag assignment as the late-takeover root cause. A
  default-off managed `prediction.conf` overlay sets Apollo's own
  `enable_interactive_tag=false`; the recorded module-start script proves the
  flag was active. Target processing changes from `220`
  `JOINTLY_PREDICTION_PLANNING_EVALUATOR` rows in the initial-state-gate
  baseline to `221 VECTORNET_EVALUATOR` rows, but the first real `18.97m/s`
  lead still has only about `0.027m/s` predicted speed at `+1s`. Formal Apollo
  ownership becomes later, not earlier: about `4.80s / 84.29m` after target
  activation versus about `4.45s / 77.44m` in the matched gate-only sample.
  The accepted v2 pair is completed/evaluable/comparable, both rows are Phase
  1 `success`, all `175` formal Planning rows are NORMAL, and collision/lane
  invasion counts are zero. This is valid negative diagnostic evidence, not a
  promoted fix or natural-driving claim.
- Apollo 10 source explains the shared result: both Jointly and VectorNet
  evaluators allocate a 20-position obstacle-history input initialized with
  zeros and fill only history that actually exists. Merely changing evaluator
  selection cannot supply a newly visible target's missing motion history.
  The next candidate must preserve real actor identity/kinematics and test a
  truthful fresh-object fallback or startup lifecycle; the strict non-fallback
  Planning gate stays unchanged. Python compile, `74` related tests, and the
  full suite (`2342 passed`) succeed.
- The first noninteractive A/B orchestration artifact without the v2 suffix is
  setup-invalid: its profile filename omitted the required
  `extended_opendrive` dispatch marker, so PlanningControl ran but Apollo had
  `missing_command`. It is not backend behavior evidence. The renamed profile
  has a launch-plan regression test and the v2 pair is the only accepted A/B.
- The candidate-only initial-state obstacle gate is online-materialized in
  `runs/phase1_online_pairs/cycle_inf_baguang_initial_state_obstacle_gate_20260716`.
  It publishes the real lead only after authored speed/gap readiness, and the
  pair remains completed/evaluable/comparable with zero safety events. It does
  not make Apollo takeover earlier: materialization-to-formal ownership is
  about `4.50s / 78.39m`. The first real `18.97m/s` target is predicted at
  only about `0.132m/s` one second ahead, isolating fresh-target Prediction
  startup as the current wait source rather than old stationary history.
- A per-tick velocity-hold diagnostic shortened compatible Planning to about
  `2.40s / 46.41m` but crossed `SolidSolid` before formal time because setup
  still owned ego with zero steer. That Apollo row is invalid setup evidence,
  not a behavior loss; the velocity-hold implementation was removed. Apollo
  Prediction process-reset attempts were likewise rejected after restart
  failure or a roughly `13.9s` Planning-output outage. The strict NORMAL gate
  remains unchanged.

- Zero-speed early-handover pair
  `runs/phase1_online_pairs/cycle_inf_baguang_planning_ready_early_handover_obstacle_source_time_20260716`
  separates ego ownership from official scenario start. The opt-in
  `planning_ready` ownership mode hands the stationary ego to Apollo as soon
  as Routing and a fresh multi-point NORMAL trajectory are present; the scene
  then controls only the lead vehicle while the formal timer continues to
  wait for the declared speed/gap condition. Online, handover occurs at sim
  `29.581s` with ego/lead both at `0m/s`, a `111`-point NORMAL trajectory, and
  a successful route. Apollo's first applied control follows at `29.681s`.
  All `600` setup trace rows control only `lead_vehicle`; none controls ego.
  This proves that the new boundary does not defer Apollo ownership until
  `70kph`.
- The same candidate preserves the ROS2 object snapshot's source time instead
  of relabeling its older position with the newer Localization time. Online,
  position-derived versus reported lead-speed residual improves from mean /
  p95 / max about `1.188 / 5.087 / 12.965m/s` in the matched localization-time
  sample to `0.025 / 0.083 / 0.357m/s`; residuals above `2m/s` fall from `77`
  to `0`. Source age at Localization is bounded at about `0.10s`. This is an
  online-proven input-contract correction in the quarantined Baguang
  candidate, not a default-profile promotion. Python compile, two exact
  tests, `111` related tests, and the carla16 full suite (`2322 passed`)
  succeed.
- The pair does not complete formal Apollo playback. After the `30s` setup
  window, ego/lead are about `15.29/15.74m/s` with `27.09m` gap, so the formal
  timer never starts. Apollo is correctly classified
  `invalid/setup_failed`, non-evaluable, and not a backend loss;
  PlanningControl succeeds. There are no collisions or lane invasions, and
  the setup trace observes NORMAL Planning throughout, but Apollo Prediction's
  three-second lead-speed projection still has large oscillations. The next
  blocker is Prediction obstacle history/acceleration semantics, not a late
  control handover. This sample does not support an Apollo natural-driving
  claim.
- Source-fresh Prediction-history follow-up
  `runs/phase1_online_pairs/cycle_inf_baguang_prediction_source_fresh_materialized_20260716`
  isolates the bridge cadence defect behind most of that oscillation. Apollo's
  jointly prediction/planning evaluator consumes adjacent obstacle positions
  from a fixed history without receiving the corresponding time deltas. The
  previous bridge also applied its own `10Hz` simulation-time rate gate to an
  already `10Hz` ROS2 objects stream, producing repeated timestamps and
  `0.2s` holes. The candidate-only `obstacle_publish_policy=source_fresh`
  instead publishes every distinct source timestamp observed by the bridge
  once. The default remains `rate_limited`; stable Town01 profiles are
  unchanged.
- The first attempted source-fresh pair
  `cycle_inf_baguang_prediction_source_fresh_20260716` was not an A/B result:
  its typed config requested `source_fresh`, but the transition and adapter
  materialization paths omitted the field and the effective bridge remained
  `rate_limited`. Both paths now preserve `obstacle_time_source` and
  `obstacle_publish_policy`. Python compile, four exact tests, `171` related
  tests, and the full suite (`2323 passed`) succeed.
- In the materialized online pair, effective bridge artifacts record
  `source_fresh`; `385` ROS2 objects JSON frames yield `384` bridge inputs and
  `383` obstacle publications. Consecutive Prediction obstacle stamps improve
  from `315 x 0.1s / 34 x 0.2s / 7 duplicates` in the immediately preceding
  rate-limited sample to `381 x 0.1s / 1 x 0.2s / 0 duplicates`. Apollo logs
  contain no invalid/older obstacle timestamp error. Three-second predicted
  speed minus current obstacle speed improves from mean absolute / p95 about
  `1.991 / 7.673m/s` with `34` samples above `5m/s` to
  `0.827 / 1.924m/s` with `2` above `5m/s`. Both remaining outliers follow the
  sole `0.2s` gap.
- This cadence repair still does not make the row evaluable. Routing succeeds
  at sim `29.420s`, a `111`-point NORMAL trajectory appears at `29.670s`, the
  stationary handover opens at `29.770s`, and Apollo control starts about
  `0.10s` later. The `600` setup rows control only the lead actor, but after
  `30s` ego/lead are about `14.72/16.35m/s` and the gap is `26.53m`, outside
  the declared `20+/-1m` setup condition. Formal time never starts; Apollo is
  `invalid/setup_failed`, non-evaluable, and not a backend loss.
- Authored-initial-state follow-up
  `runs/phase1_online_pairs/cycle_inf_baguang_authored_initial_speed_gate_20260716`
  closes that route-budget blocker without treating scene acceleration as
  Apollo behavior. The canonical case declares ego and lead initial speed as
  `19.44m/s`; the prior zero-speed setup consumed about `304m` even though the
  `398.75m` route must retain about `287.5m` for the authored 18-second lead
  profile. A default-off runtime option now performs one one-shot assignment
  of the authored initial velocities only after the strict
  `apollo_planning_ready` gate. It then retains the existing target-ready,
  speed-compatible replan plus distinct-NORMAL confirmation before allowing
  Apollo Control or formal scenario time. This is setup materialization, not
  an Apollo `0 -> 70kph` claim.
- Online, Apollo's real RoutingResponse succeeds at sim `29.361s`, the first
  `111`-point NORMAL trajectory appears at `29.611s`, and the materialization
  gate opens at `29.711s`. Ego position is unchanged before the gate and lead
  position varies by less than `1mm`; both gate-side observed speeds are
  `0m/s`. The first post-gate setup observation is at `29.761s`. Setup reaches
  a compatible `seq=120` NORMAL replan plus `seq=121` NORMAL confirmation at
  `33.711s`; Apollo's first published control and formal scene start follow at
  `33.811s`. Setup therefore uses `4.10s / 70.58m`, versus the prior
  `30s / about 304m` timeout. The handover marker records zero pre-handover
  Apollo control publications, ego/lead about `19.50/19.50m/s`, and a
  `20.005m` gap.
- The atomic velocity change causes `10` SPEED_FALLBACK messages during setup,
  but none opens the handover: the setup window contains `31 NORMAL / 10
  SPEED_FALLBACK`, while all `179` formal-window Planning messages are NORMAL.
  The complete lead profile finishes at `18.00s`; the actor contract passes,
  initial-state materialization passes with about `0.016m/s` error, both rows
  are completed/evaluable, and comparison is `comparable`. PlanningControl and
  Apollo are both labeled Phase 1 `success`, with zero collision and lane
  invasion. Python compile, `git diff --check`, four exact tests, `103`
  related tests, and the full suite (`2326 passed`) succeed.
- This is not representative longitudinal success or an Apollo
  natural-driving claim. Apollo's formal bumper gap grows from about `20.01m`
  to `45.15m`; final ego/lead speeds are about `10.70/19.18m/s`, and Control
  health is `warn`. The option remains candidate-only, does not change the
  stable Town01 profiles, does not default physical mapping, and does not tune
  `steer_scale`. The next runtime blocker is reproducible post-handover
  longitudinal fidelity and candidate portability, not readiness or route
  budget.
- Destination-rule A/B pair
  `runs/phase1_online_pairs/cycle_inf_baguang_destination_rule_ab_20260716`
  disproves the initial hypothesis that Planning's late `stop by DEST`
  decision causes the weak recovery. The diagnostic candidate removes only
  `DESTINATION`; the other eight traffic rules remain present. Both rows are
  completed/evaluable, comparison is `comparable`, and Apollo completes the
  full actor profile with zero collision/lane invasion. Its formal Planning
  window is `180 NORMAL / 0 fallback`, with no main stop decision, but the
  bumper gap still grows from about `20.01m` to `45.81m` and final ego/lead
  speeds remain about `10.96/19.18m/s`. The rule crop is therefore rejected as
  a fix or promotion candidate. Three exact tests, `76` related tests, and the
  full suite (`2329 passed`) succeed.
- The A/B trace confirms that the setup ownership interval is materially too
  long for the available Baguang road budget. The gate opens at sim
  `29.709s`, while Apollo Control and formal time start at `33.959s`; during
  those `4.25s`, lead advances about `74.48m`. At formal `t=14.7s`, Apollo
  Prediction first clamps the lead trajectory at the Baguang lane endpoint
  near `x=-104.977m`. At `t=18s`, the lead is already near `x=-85.530m` and
  Prediction's nominal `5.9s` future covers only about `19.45m` before that
  same endpoint, rather than the roughly `113m` implied by current speed.
  Planning consequently follows an obstacle predicted to stop at the map
  boundary even though the published current lead velocity is correct.
- Shortening setup is necessary but not sufficient: even with zero setup
  travel, the authored 18-second profile plus the high-speed Prediction
  horizon exceeds the remaining straight by about `29m`. With the current
  lifecycle, the combined deficit is about `104m`; a clean map-level candidate
  should extend both the CARLA and Apollo Baguang lane/route by at least
  `110-120m` before repeating the pair. An alternative is an explicit,
  backend-symmetric ScenarioCase redesign that declares the initial cruise as
  setup and starts scoring at the dynamic stimulus. Letting the scene own ego
  for part of the existing formal case is useful only as assisted diagnostic
  evidence; it cannot support full Apollo-ownership or natural-driving
  evidence.
- The isolated matched-map candidate has now been exercised online. CARLA
  loads a generated straight extended by `120m` at both ends, while Apollo
  loads a separate `OpenDriveMap` with `125m` driving-lane extensions. The
  first attempt serialized each extension as one long segment; the bridge's
  trusted parser correctly rejected segments over `25m`, snapped Routing
  start `119.664m` forward, and produced `898/898` empty Planning messages.
  That Apollo row is invalid setup evidence, not a behavior loss.
- The HDMap converter now samples only the new extension at no more than
  `20m` spacing. Python compile, `14` focused tests, and the full suite
  (`2335 passed`) succeed. In
  `runs/phase1_online_pairs/cycle_inf_baguang_extended_opendrive_densified_hdmap_20260716`,
  Routing succeeds at sim `10.840s` with start-projection error about
  `3.9e-7m`; a `111`-point NORMAL trajectory follows at `11.040s`. Both
  actors stay stationary until after that strict gate. The pair is
  completed/evaluable/comparable, both Phase 1 rows are `success`, the full
  actor profile completes, and collision/lane-invasion counts are zero.
- All `177` formal Apollo Planning messages are NORMAL. The final lead is near
  `x=164.27m`, Prediction carries it through a full `5.9s` horizon to about
  `x=263.93m`, and the lane continues to about `x=424.70m`; the finite-map
  clamp is absent. Apollo's valid bumper gap has minimum/final about
  `17.42/40.03m`, with final ego/lead speeds about `15.10/19.18m/s`.
- This does not remove the ownership concern. Formal time/Apollo handover is
  at `15.340s`, about `4.3s` after the first NORMAL trajectory; ego and lead
  each travel about `72.5m` while the setup layer establishes the authored
  `19.44m/s` initial condition and waits for a compatible replan. The current
  case therefore validates a `70kph` initial-state dynamic event, not Apollo
  acceleration from rest. If `0 -> 70kph` is a required measured behavior,
  it needs a separate backend-symmetric ScenarioCase with formal initial speed
  zero and Apollo ownership from its first valid Planning handover.

- Replan-confirmed handover pair
  `runs/phase1_online_pairs/cycle_inf_baguang_replan_confirmed_handover_20260716`
  tests whether the setup controller was retaining ego ownership longer than
  needed. The quarantined candidate replaces the arbitrary ten-distinct-
  message streak with a compatible fresh NORMAL replan plus one subsequent
  compatible distinct NORMAL message. Fallback, stale, short, and
  speed-incompatible trajectories still block or reset the gate. The option is
  default-off and does not change `steer_scale=1.0`, longitudinal mapping,
  guards, or the stable Town01 profiles. Python compile, three exact tests,
  `138` related tests, and the carla16 full suite (`2318 passed`) succeed.
- Online, Routing succeeds at sim `29.290s` before setup motion begins at
  `29.640s`. Target-speed readiness occurs after `7.65s`. A compatible replan
  at `seq=168` is correctly rejected when `seq=169 SPEED_FALLBACK` follows;
  the final ownership transfer is anchored by `seq=183 NORMAL / 111 points`
  and confirmed by `seq=184 NORMAL / 119 points`. Handover occurs after
  `11.40s`, versus `13.45s` in the coherent-time ten-message baseline, and
  formal playback starts `0.20s` later. This removes about `2.05s / 40m` of
  setup-owned travel. The marker records zero pre-handover Apollo control
  publications, ego/lead about `19.53/19.55m/s`, gap about `19.93m`, and a
  non-fallback speed-compatible trajectory.
- Both dispatches complete, both rows are evaluable, comparison is
  `comparable`, and there are no invalid rows. PlanningControl succeeds.
  Apollo has zero collisions but is a valid behavior `failed/lane_invasion`:
  after `4.85s` of formal playback, CTE has grown from about `+0.12m` to
  `-0.824m` and heading error to about `-0.062rad`. The formal Planning window
  contains `44 NORMAL / 1 SPEED_FALLBACK`; the shortened v-t-gap trace grows
  from about `19.95m` to `26.54m` before termination. The candidate therefore
  proves the replan-confirmed ownership boundary and shorter setup interval,
  but it is not promoted and does not prove behavior improvement or Apollo
  natural driving.

- Coherent-time fresh-GT pair
  `runs/phase1_online_pairs/cycle_inf_baguang_fresh_gt_coherent_simtime_20260716`
  isolates one variable from the 2026-07-15 fresh-GT candidate: Apollo mock
  `/clock`, Localization/Chassis headers, and obstacle headers now all use
  CARLA simulation time. The strict ten-distinct-message speed-compatible
  NORMAL handover, steering-only physical-domain candidate, legacy
  longitudinal mapping, disabled guards, and `steer_scale=1.0` are unchanged.
  The two exact tests, `43` related tests, and the carla16 full suite
  (`2317 passed`) succeed.
- Online, a `398.75m` RoutingResponse at sim `30.222s` and the first
  `111`-point NORMAL trajectory at `30.722s` precede setup-preroll motion at
  `30.872s`. The actual runtime gate is `apollo_planning_ready`; official
  actor playback and evaluation start only at `44.472s`, after handover.
  Apollo publishes no CARLA control before ownership transfer. At handover,
  `seq=204` is NORMAL with `137` points, ego/lead are about
  `19.54/19.55m/s`, gap is about `19.96m`, and current/one-second planned
  speeds are about `19.33/18.38m/s`.
- Relative to the otherwise matched fresh-GT wall-time sample, coherent time
  removes all five pre-roll `current time smaller than the previous
  trajectory` replans, reduces the pre-roll Planning mix from `131 NORMAL / 9
  SPEED_FALLBACK` to `117 NORMAL / 6 SPEED_FALLBACK`, and advances handover
  from `15.60s / 229.43m` to `13.45s / 187.40m`. This is a real improvement,
  but it still consumes about `47%` of the `398.75m` route before Apollo owns
  ego. Eight pre-roll matched-point `lon_diff` replans remain because the
  fixed setup controller holds measured speed near `19.5m/s` while stitched
  NORMAL trajectories repeatedly decay toward roughly `12-15m/s`.
- The pair is completed/evaluable/comparable with no invalid row;
  PlanningControl succeeds. Apollo has zero collision/lane invasion and
  formal Planning is `170 NORMAL / 2 SPEED_FALLBACK`, but remains
  `degraded/large_final_gap`: final ego speed is about `2.95m/s` and final gap
  about `122.02m`. Coherent time is therefore retained only as a quarantined
  diagnostic candidate. It does not prove acceptable handover latency,
  longitudinal behavior, candidate promotion, or Apollo natural driving.

2026-07-15 targeted online evidence:

- Fresh-GT pair
  `runs/phase1_online_pairs/cycle_inf_baguang_fresh_gt_candidate_20260715`
  closes the upstream duplicate-timestamp blocker exposed by the guard-free
  speed-handover sample. The inherited bridge template allowed cached GT state
  republish between CARLA frames, producing `37` repeated Localization and
  Chassis timestamps. A quarantined one-variable candidate changes only these
  channels to `skip / once_per_new_sim_frame`; it does not change Planning,
  Control, steering scale, longitudinal mapping, guards, or scenario ownership.
- Online, runtime detects and skips `57` cached iterations and publishes
  `749/749` fresh Localization/Chassis messages with zero duplicate
  timestamps. Localization becomes non-blocking `warn`, channel cadence is
  non-blocking, and Control health changes from
  `fail/apollo_raw_command_oscillation` to `warn/control_health_warn` as
  raw/mapped/applied throttle-brake switches decrease from `13` to `9`.
  Routing and a fresh `137`-point NORMAL trajectory precede the same ten-message
  speed-compatible handover at ego/lead about `19.55/19.55m/s`; zero Apollo
  control publications occur before ownership transfer. Formal Planning is
  `176 NORMAL / 0 fallback`.
- The pair is completed/evaluable/comparable and both rows have zero collision
  and lane invasion, but Apollo remains `degraded/large_final_gap`: final ego
  speed is about `2.28m/s`, final gap about `154.00m`, and formal CTE p95/max
  grows to about `0.526/0.529m`. The candidate therefore remains quarantined.
  Here `claim_grade` means strict GT timestamp publication only; it is not an
  Apollo natural-driving claim. The two exact tests, `41` related tests, and
  the carla16 full suite (`2315 passed`) succeed.

- The requested setup-owned acceleration model is now exercised as a strict
  pre-roll contract: the scene player can accelerate ego and lead from rest to
  about `70kph` while Apollo observes live inputs in shadow. Official scenario
  time and backend evaluation start only after Routing succeeds and Apollo
  presents ten distinct fresh NORMAL trajectories whose current and
  one-second-lookahead speeds are compatible with measured ego speed. This
  makes the initial-speed setup reproducible; it must not be reported as
  Apollo naturally accelerating from zero.
- Guard-free pair
  `runs/phase1_online_pairs/cycle_inf_baguang_handover_speed_physical_steering_guard_free_20260715`
  keeps that speed-compatible handover and isolates the subsequent early lane
  departure to the legacy bidirectional steering full-scale contract. It maps
  steering output and measured feedback through the same `30deg` Apollo
  front-wheel domain, while leaving longitudinal mapping legacy and
  `steer_scale=1.0`. Every lateral guard is explicitly disabled and runtime
  guard apply counts are zero.
- Relative to the matched legacy speed-gate run, first-`2.7s` CTE p95/max
  changes from about `0.440/0.656m` to `0.063/0.071m`, and heading-error
  p95/max changes from about `0.151/0.164rad` to
  `0.0079/0.0084rad`. The Apollo row completes all `18.0s` with zero collision
  and lane invasion, but remains `degraded/large_final_gap`: ego slows from
  about `19.29m/s` to `12.14m/s`, final gap reaches about `118.29m`, and formal
  Planning contains `161 NORMAL / 4 SPEED_FALLBACK`. The local/ignored
  calibration, one-scenario coverage, and unresolved longitudinal behavior
  keep this profile quarantined. The final guard-free config passes `39`
  focused tests and the carla16 full suite (`2313 passed`). Defaults and
  Town01 lateral profiles are unchanged; this is not an Apollo
  natural-driving claim.

- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_handover_speed_compatibility_20260715`
  closes the next rolling-handover gap. The earlier gate accepted a fresh
  NORMAL label without proving that its speed profile matched the physically
  pre-rolled ego, and tick-based holds could count the same Planning message
  more than once.
- Planning diagnostics now expose the first nonexpired speed and nearest
  one-second lookahead speed. A default-off setup gate can compare both with
  measured ego speed and count distinct compatible Planning sequences. The
  quarantined candidate uses `1.0m/s` current tolerance, `2.0m/s` lookahead
  tolerance, and ten distinct messages. It never edits Apollo output and is
  not a behavior assist. Python compile, `122` targeted tests, and the carla16
  full suite (`2311 passed`) succeed.
- Online, the `398.75m` RoutingResponse and a 111-point NORMAL Planning
  trajectory precede setup-preroll ego/lead motion. Handover waits until
  `seq=191 NORMAL` with `141` points: ego/lead are about
  `19.54/19.55m/s`, gap is `19.94m`, and current/one-second planned speeds are
  about `19.16/19.10m/s`. Formal scene time starts after the first Apollo
  control publication.
- The pair is completed/evaluable/comparable. PlanningControlBackend succeeds;
  Apollo is a valid `failed/lane_invasion` row with zero collision and no
  invalid reasons. Its 2.7-second formal window contains `21 NORMAL` and two
  PATH_FALLBACK Planning messages, with no SPEED_FALLBACK. This proves the
  setup handover, not the full scenario or Apollo natural driving; the active
  five-P0 matrix and default mapping remain unchanged.

- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_latest_planning_snapshot_gate_20260715`
  closes a cross-process readiness-materialization race. The fixed-scene hook
  previously read `cyber_bridge_stats.json`, which is flushed every `0.1s`;
  in the first bidirectional-steering sample its handover marker still
  reported `seq=152 NORMAL` after Planning had emitted `seq=153/154
  SPEED_FALLBACK`. The bridge's exact non-fallback publish gate prevented
  those fallback Control inputs from reaching CARLA, but the scene marker was
  not a truthful snapshot of the latest Planning callback.
- The bridge now includes `last_msg_wall_time_sec` in the atomically replaced
  per-message `planning_topic_debug_summary.json`. The scene gate retains
  Routing and cumulative counters from bridge stats but selects the
  per-message Planning snapshot when its receipt timestamp is newer. Targeted
  tests cover stale-stats NORMAL versus newer fallback and subsequent newer
  NORMAL transitions; `101` related tests and the full `2307`-test suite pass.
- Online, RoutingResponse arrives at sim time about `29.358s`, before physical
  pre-roll begins around `29.758s`. At the handover marker, stats still report
  `seq=154`, while the selected per-message snapshot is `0.115s` newer and
  reports `seq=155 NORMAL`, `137` points, and message age about `0.095s`.
  The bridge's first publish opening then exact-matches `seq=156 NORMAL`;
  `control_tx_count_before_handover=0`, and official scenario time begins
  `0.20s` after the marker.
- Both backend rows are completed, evaluable, Phase 1 `success`, and the pair
  is comparable with no invalid rows. Apollo has zero collision/lane invasion,
  all `163` formal Planning messages are NORMAL, all `361` v-t-gap rows use
  normal bumper projection, and there are three `lon_diff` replans. This
  validates latest-message readiness materialization, not longitudinal
  recovery: just before handover Apollo Control already requests about `41%`
  brake and `-5.39m/s2` from a NORMAL trajectory whose speed reference is
  about `15.29m/s` while ego is about `19.04m/s`; the first formal replan at
  `+0.25s` reports `lon_diff=-2.67585m`. Ego finishes around `9.01m/s` versus
  lead `19.18m/s`, with gap about `49.74m`, and Control health remains `warn`.
  No default physical mapping or Apollo natural-driving claim follows.

- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_bidirectional_steering_20260715`
  isolates the first stitched-run lateral failure to a bidirectional steering
  full-scale mismatch. Apollo Chassis/Control interprets steering percentage
  in the configured Apollo front-wheel range (about `30deg` here), while the
  legacy bridge path applied the normalized Apollo command directly to the
  CARLA Lincoln's larger actuator range and normalized measured CARLA wheel
  angle back by that CARLA range. In the failed legacy window, measured
  feedback therefore had absolute p95 about `26.5%` where the same wheel
  angles represent about `61.7%` in Apollo's configured domain, while the
  output-side legacy command had about `2.2x` the calibrated steering
  authority.
- The isolated candidate explicitly enables physical mapping for steering
  only, maps measured wheel-angle feedback through the same `30deg` Apollo
  domain, keeps longitudinal throttle/brake on the inherited legacy path,
  leaves `steer_scale=1.0`, and preserves strict readiness, exact NORMAL
  handover, coherent sim time, and trajectory stitching. Runtime artifacts
  materialize `actuator_mapping_mode=physical`, `map_steering=true`,
  `map_steering_feedback=true`, no physical fallback, and
  `steering_feedback_normalization=apollo_front_wheel_angle_percent`.
- Relative to the legacy stitcher pair's first `4.4s`, cross-track-error
  p95/max changes from about `0.612/0.657m` to `0.059/0.063m`, and heading
  error p95/max changes from about `0.192/0.222rad` to
  `0.0081/0.0091rad`. The new pair completes the full 18-second actor profile
  with zero collision and lane invasion; both rows are completed, evaluable,
  and Phase 1 `success`, and the comparison is `comparable /
  apollo_vs_planning_control_evaluable`. Formal Planning has `163 NORMAL`, no
  fallback trajectory, `161` preserved trajectories, and two longitudinal
  difference replans.
- This closes the early stitched lateral-regression attribution for this one
  Baguang sample, not candidate promotion. Full-window maximum CTE still
  reaches about `0.667m`, Control health remains `warn`, final ego speed is
  about `7.92m/s` versus lead `19.16m/s`, and final bumper gap is about
  `48.29m`. `chassis_gt_contract` is `insufficient_data` because runtime
  Chassis samples are missing from the claim-grade contract. The calibration
  file is local/ignored, and Baguang/Town01 cross-scenario no-regression
  evidence is required. The active P0 profile and five-P0 matrix remain
  unchanged; no default physical mapping or Apollo natural-driving claim
  follows.
- Same-config repeat
  `runs/phase1_online_pairs/cycle_inf_baguang_bidirectional_steering_repeat_20260715`
  confirms the early steering-domain effect but does not reproduce the first
  pair's Phase 1 `success` label. Handover again uses a fresh 134-point NORMAL
  trajectory with ego/lead about `19.11/19.10m/s`, gap about `19.82m`, and
  zero pre-handover Apollo control publications. The first `4.4s` CTE p95/max
  is about `0.086/0.087m` and heading-error p95/max is about
  `0.0074/0.0077rad`; the full formal window has CTE p95/max about
  `0.134/0.587m`, with zero collision and lane invasion.
- The repeat remains completed, evaluable, and comparable, but Apollo is
  `degraded/degraded_gap_method`: `6/361` gap rows become lateral-degraded as
  the large longitudinal separation combines with late heading error. Final
  ego/lead speeds are about `8.74/19.18m/s` and final gap is about `53.45m`.
  Formal Planning is still fallback-free (`162 NORMAL`), with `159`
  non-replans and three `lon_diff` replans. The repeat therefore supports the
  early bidirectional steering diagnosis while strengthening the quarantine:
  stable Phase 1 status and post-handover longitudinal/trajectory continuity
  are not yet reproducible.

- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_trajectory_stitcher_20260715`
  materializes a Baguang-only trajectory-stitching A/B while keeping
  reference-line stitching disabled and preserving coherent time, the 0.1s
  acceleration-feasibility constraint, legacy mapping, steering, and the
  strict handover gate. The pair is completed, evaluable, and comparable;
  PlanningControl succeeds, while Apollo is a valid `failed/lane_invasion`
  behavior outcome rather than an invalid run.
- The A/B confirms a longitudinal continuity mechanism. Over the first 4.4s,
  the matched stitcher-off candidate has `38 NORMAL / 2 SPEED_FALLBACK` and
  all 40 Planning messages replan because stitching is disabled; ego reaches
  `2.92m/s`. With trajectory stitching enabled, the window has
  `37 NORMAL / 1 PATH_FALLBACK`, 37 messages preserve stitching, ego bottoms
  at `8.08m/s` near `+2.35s`, and recovers to about `10.97m/s` at `+4s`.
- The candidate is not promotable. Apollo crosses a Broken marking at
  `+4.40s`; the vehicle footprint intersects the marking, so the lane hard
  gate is valid. Same-window maximum CTE grows from `0.138m` to `0.657m` and
  heading error from `0.041rad` to `0.222rad`. The sole replan is triggered by
  matched-point `lat_diff=-0.582932` and produces PATH_FALLBACK. Raw, mapped,
  and applied steer are consistent, and Control's target remains on the
  Planning path-candidate line. That evidence rules out dropped, guarded, or
  sign-inverted actuation, but it did not rule out a bidirectional full-scale
  mismatch; the steering-only A/B above supersedes the earlier mapping
  attribution.

- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_coherent_simtime_accel_feasibility_20260715`
  validates the existing setup-only scene-to-Apollo handover with a narrowly
  isolated localization-acceleration feasibility candidate. The scene player
  physically accelerates ego and lead from rest to about `70kph` while Apollo
  receives live inputs in shadow; the marker records ego/lead
  `19.36/19.38m/s`, bumper gap about `19.82m`, a fresh 111-point NORMAL
  trajectory, and zero Apollo control publications before atomic handover.
  Official scenario time and backend evaluation begin only after handover.
- The matched coherent-simtime baseline exposes negative projected Planning
  replan states such as `v=-0.0088m/s, a=-8.80m/s2` and
  `v=-0.784m/s, a=-7.88m/s2`. Apollo's 0.1-second forward projection can
  therefore violate the Piecewise Jerk optimizer's nonnegative-speed bound.
  The candidate preserves measured acceleration and adds only the existing
  `nonnegative_speed_prediction_horizon_s=0.1` constraint; it does not change
  control mapping, steering, PID, physical mapping, or the active P0 profile.
- The run is completed, evaluable, and comparable with no collision or lane
  invasion. Runtime stats materialize the candidate and record `23`
  nonnegative-speed corrections. Post-handover Planning improves from the
  matched baseline's `135 NORMAL / 20 SPEED_FALLBACK` to
  `145 NORMAL / 11 SPEED_FALLBACK`.
- This is not a behavior repair or promotion. Ego still falls from about
  `19.38m/s` to `4.24m/s` by `+4s` and about `0.08m/s` by `+18s`; bumper gap
  grows to about `248.64m`, so Apollo remains `degraded`. The candidate
  Planning INFO artifact is only a bounded tail and cannot prove that all QP
  infeasibility disappeared. Remaining work is the post-obstacle Planning
  trajectory/source-control recovery chain, not setup ownership or bridge
  actuation mapping.

- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_coherent_simtime_materialized_v2_20260715`
  proves Apollo mock clock can unify the Baguang runtime without the
  obstacle-only mixed-clock shortcut. The LaunchPlan uses the explicit
  coherent-simtime candidate; effective config sets Localization and obstacles
  to CARLA sim time and publishes 643 increasing Cyber `/clock` messages.
  Planning headers advance in that domain at a stable one-tick offset, and
  nearest-by-sim obstacle-minus-Planning header delta has median `0s`.
- The pair is completed, evaluable, and comparable. PlanningControl succeeds;
  Apollo has zero collision/lane invasion and is `degraded`, not invalid.
  Relative to the full-profile baseline, post-handover Planning changes from
  `107 NORMAL / 40 SPEED_FALLBACK / 1 PATH_FALLBACK` to
  `135 NORMAL / 20 SPEED_FALLBACK`, and applied throttle/brake switches fall
  from `69` to `53`.
- This is not a longitudinal pass. Ego still falls from about `19.39m/s` to
  `3.20m/s` by `+4s` and about `0.08m/s` at `+18s`; gap grows to about
  `248.94m`. The first post-start Planning message is SPEED_FALLBACK and later
  NORMAL follow trajectories also request deceleration. The coherent-clock
  profile therefore remains an isolated candidate pending repeat and
  no-regression evidence.
- The first attempted coherent-clock directory is excluded: runtime inspection
  showed Baguang legacy dispatch silently replaced the selected YAML with the
  base profile, so the process was stopped as config-not-materialized. The
  backend now forwards explicit tracked
  `phase1_baguang_apollo_dynamic_*.yaml` algorithm profiles while preserving
  the registry/default fallback; the `v2` run proves that forwarding online.

- Baguang Prediction input timing has a proven mixed-clock defect. In
  `cycle_inf_baguang_full_profile_road_budget_20260715`, PerceptionObstacle
  headers are CARLA sim time while Planning headers are Cyber epoch time;
  nearest obstacle-minus-Planning header differences have median about
  `-1784054524.61s`. Apollo 10's jointly prediction/planning evaluator uses
  these timestamps to align obstacle history with the previous trajectory.
- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_obstacle_header_alignment_materialized_20260715`
  proves an obstacle-only Cyber-time header override is materialized while
  localization/chassis retain the established `auto`/sim-time policy. Header
  delta moves to median about `0.052s` (range about `[-0.099, 0.115]s`), and
  the post-handover Planning window contains `32` NORMAL and `4`
  PATH_FALLBACK messages with no SPEED_FALLBACK. The pair is still comparable
  and evaluable, but Apollo has a real progressive lane invasion at scene time
  `4.0s`; PlanningControl succeeds. This is a valid behavior regression, not
  an invalid run or a longitudinal pass.
- The bridge retains explicit obstacle-header time-source support with the
  backward-compatible default `localization_time`. The Cyber-time override is
  removed from the active P0 dynamic profile and isolated in
  `phase1_baguang_apollo_dynamic_obstacle_cybertime_candidate.yaml`. Lane-event
  evidence shows raw/mapped/applied steer consistency and a Control target on
  the Planning path candidate, so the next cycle stays on coherent time-domain
  and Planning/lateral feedback attribution; it must not tune `steer_scale`,
  enable physical mapping, or weaken the hard lane gate.

- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_full_profile_road_budget_20260715`
  closes the road-budget/setup-invalid blocker exposed by the strict handover
  sample below. The Apollo request uses a `390m` same-lane goal and the real
  RoutingResponse covers `398.75m` on lane `0_0_2`, instead of ending near the
  old `s=300m` scene stop.
- The setup remains continuous and physical: no reset or teleport is used.
  Two consecutive fresh NORMAL observations end the rolling pre-roll after
  `7.80s`; the formal target trace starts near CARLA route `s=112.60m`, versus
  `s=174.18m` in the previous sample. At handover ego/lead are
  `19.28/19.29m/s`, gap is about `19.82m`, Planning is a fresh 111-point
  NORMAL trajectory, and `control_tx_count_before_handover=0`.
- The canonical `lead_decel_accel` stop condition is now the complete authored
  `18s` profile rather than the incorrect intermediate `s=300m` road-end
  assumption. Apollo's lead trace reaches `18.00s` and `s=397.42m`; both the
  40kph deceleration target and the final 70kph restoration are observed.
  PlanningControl also completes the same 18-second profile.
- Both runtime rows are completed and evaluable, the comparison is
  `comparable / apollo_vs_planning_control_evaluable`, and neither row records
  a collision or lane invasion. Apollo is `degraded` only because 175/361
  v-t-gap rows use the lateral-degraded longitudinal projection; it is no
  longer invalid for incomplete actor playback.
- This is lifecycle and comparison-surface success, not Apollo longitudinal
  success. After handover Apollo rapidly brakes from about `19.43m/s` to near
  zero in about four seconds, then oscillates between brake and throttle.
  Planning contains `130` NORMAL, `100` SPEED_FALLBACK, `1` PATH_FALLBACK,
  and `82` UNKNOWN rows; Planning logs contain 32 Piecewise Jerk speed-optimizer
  failure lines and control health reports 69 applied throttle/brake switches.
  The moving lead obstacle contract passes, so the next blocker is the
  Planning speed-constraint/fallback to source-control oscillation chain, not
  road budget or initial actor-state materialization.

- Pair
  `runs/phase1_online_pairs/cycle_inf_baguang_exact_normal_gate_materialized_20260715`
  closes the remaining runtime-materialization gap in the Baguang setup
  lifecycle. The active bridge config and runtime stats both show that first
  publication requires an exact Planning-sequence match and rejects
  `SPEED_FALLBACK`, `PATH_FALLBACK`, and `UNKNOWN` at handover.
- The observed Apollo routing response is at wall time about
  `1784052937.509s`; the first eligible 111-point `NORMAL` trajectory follows
  at about `1784052937.793s` / sim time `29.650s`. The first setup pre-roll
  observation is later at sim time `29.850s`, and ego/lead first exceed
  `0.1m/s` together at about `30.450s`. Thus routing and fresh NORMAL Planning
  precede both actor motion and official scene timing.
- The setup handover marker is written at sim time about `41.000s`, with ego
  and lead at `19.53/19.55m/s`, bumper gap about `19.93m`, a fresh 111-point
  NORMAL trajectory, and `control_tx_count_before_handover=0`. The bridge
  suppresses an early unmatched Control event, then opens on exact-matched
  sequence `189` with trajectory type `NORMAL`; formal scene time starts later
  at about `41.200s`.
- Both backend runtimes complete, Apollo has 134 valid v-t-gap rows, and
  neither row records a collision or lane invasion. Raw runtime evidence closes
  the readiness/ownership blocker. Before the speed-profile contract was
  hardened, postprocess incorrectly labeled both rows evaluable and the pair
  `comparable`; the corrected classification is described below. Apollo's raw
  behavior remains poor: Planning contains `81` NORMAL,
  `86` SPEED_FALLBACK, `1` PATH_FALLBACK, and `81` zero-point UNKNOWN
  messages, and final bumper gap is about `95.99m` after ego stops.
- The physical pre-roll still consumes formal-scene road budget. The Apollo
  actor trace starts near route `s=174.18m` and reaches the `s=300m` stop
  trigger after only `6.65s`, while lead target speed is still about
  `16.00m/s`; the declared 70-to-40-to-70 profile has not completed. The
  original analyzers nevertheless marked the interaction evaluable/comparable.
  `scenario_actor_contract` now checks the last distinct speed-profile
  transition and the declared deceleration/restoration targets. Reprocessing
  the same online evidence keeps PlanningControl `success` because it completes
  the last transition at `17.0s`, but classifies Apollo
  `invalid/fixed_scene_failed`; the pair is now `partially_evaluable` with a
  missing evaluable Apollo row. This is setup/evidence failure, not a backend
  behavior loss. Targeted tests report `102 passed`; the full suite reports
  `2297 passed`.

2026-07-14 targeted online evidence:

- Fresh five-P0 matrix
  `runs/phase1_p0_matrix/cycle_inf_mainline_vehicle_param_map_speed_20260714`
  reports `comparable_pair_count=5`, `invalid_pair_count=0`, and
  `partially_evaluable_pair_count=0`. PlanningControlBackend is successful in
  5/5 rows. Apollo is successful in `lane_keep_straight` and
  `lane_keep_curve`, fails `follow_stop_static` with lane invasion, and fails
  `lead_decel_accel` and `cut_in_simple` with collision. These are valid
  behavior outcomes, not setup losses.
- Targeted Town01 runs under
  `runs/phase1_online_pairs/cycle_inf_mainline_vehicle_param_20260714` remain
  bounded in the observed windows. `lane_keep_097` records about `188.13m`
  route distance, lateral-error p95 about `0.247m`, heading-error p95 about
  `0.0428rad`, and zero collision/lane invasion. The curve217 diagnostic
  records about `148.84m`, lateral-error p95 about `0.155m`, heading-error p95
  about `0.0127rad`, and zero collision/lane invasion. Localization,
  reference-line, link-health, and claim-boundary warnings still prevent a
  no-interference natural-driving claim.
- Targeted Baguang dynamic pair
  `runs/phase1_online_pairs/cycle_inf_baguang_vehicle_param_20260714` exposes
  an Apollo startup-lifecycle blocker rather than a comparison-platform
  blocker. The scene starts lead/ego motion before Apollo has a normal
  trajectory; Planning produces only two non-empty one-point
  `SPEED_FALLBACK` trajectories out of 84 messages, followed by collision.
  At that point the Planning-ready scene-start gate had only offline tests;
  the follow-up evidence below supersedes that pending status.
- Follow-up pair
  `runs/phase1_online_pairs/cycle_inf_baguang_planning_ready_stats_flush_20260714`
  validates that lifecycle gate online. Lightweight bridge stats are now live
  at one-second cadence for this dynamic profile. Routing succeeds at sim time
  about `32.33s`, the first eligible 111-point `NORMAL` trajectory arrives at
  about `32.83s`, and the fixed-scene phase starts later at about `33.43s`.
  Apollo completes the storyboard with zero collision/lane invasion and the
  pair is evaluable and comparable. This is lifecycle success, not complete
  longitudinal behavior success: Apollo remains `degraded`, Planning still
  contains fallback output, and bumper gap grows from about `20.01m` to
  `225.93m`.
- A matched follow-up pair,
  `runs/phase1_online_pairs/cycle_inf_baguang_obstacle_yaw_units_20260714`,
  corrects a GT input-contract error: `objects_gt_json` yaw is expressed in
  degrees but was previously passed directly to Apollo's radian-valued
  `PerceptionObstacle.theta`. The new run remains evaluable/comparable with
  zero collision/lane invasion and preserves the Planning-ready gate. The
  pronounced ego slowdown is delayed from about scene second `3` to `4`, and
  final bumper gap improves from about `225.93m` to `214.08m`. Apollo still
  approaches zero speed by about second `6`, retains fallback Planning, and
  remains `degraded`; the unit correction is online-proven but is not a
  longitudinal behavior fix.
- A subsequent dynamics probe,
  `runs/phase1_online_pairs/cycle_inf_baguang_vehicle_dynamics_probe_20260714`,
  localizes the next startup-state discontinuity. The ego is moving at the
  injected `19.44m/s` while CARLA still reports automatic gear `0`; at about
  scene second `2.90`, gear changes to `1` and longitudinal acceleration drops
  to about `-19.035m/s2` despite positive throttle, zero brake, no hand brake,
  and near-zero steer. Path curvature and Planning speed-limit collapse follow
  this event. This is stronger evidence than the rejected asynchronous manual
  gear-handoff attempts and motivates physical setup acceleration from rest.
- The v3 physical-pre-roll pair
  `runs/phase1_online_pairs/cycle_inf_baguang_physical_preroll_handover_v3_20260714`
  first demonstrated setup-only acceleration and separate official timing,
  but its one-second stats refresh and lack of explicit Planning message-age
  checking allowed a stale NORMAL snapshot to support the marker. It therefore
  does not prove exact non-fallback first-control handover. The 2026-07-15 pair
  above supersedes that lifecycle claim with explicit freshness and exact
  sequence evidence. The v3 post-handover lane invasion and gap growth remain
  diagnostic behavior evidence only.

2026-07-11 targeted online evidence:

- The Baguang dynamic-scene speed ceiling was traced to inconsistent Apollo
  map derivatives, not an unloaded Planning flag. Apollo selects
  `base_map.bin` before `base_map.txt`; the old binary and `sim_map.*` encoded
  `4.9671m/s`, while a later text-only patch showed `23.61m/s`. The RoadRunner
  converter now writes the km/h value expected by Apollo's XML parser before
  atomically generating base/sim/routing outputs, and the build fails if the
  generated driving-lane speeds differ from the requested value. In the
  matched online repeat, Planning `speed_limit_from_ref` changed from
  `4.9671` to `23.61m/s` and ego peak speed changed from about `5.00` to
  `14.99m/s`. The run is still invalid for the dynamic case because ego did
  not reach the explicit 18m/s scene-arm threshold before a high-speed lane
  invasion. This proves the map-speed repair, not lead-decel-accel success.
- A follow-up Baguang `lead_decel_accel` steering-only calibrated A/B reached
  `19.54m/s` with maximum observed cross-track error about `0.046m` and zero
  lane invasions, compared with the legacy-mapping repeat that invaded near
  `14.99m/s` with maximum cross-track error about `0.825m`. This is strong
  evidence that the existing steering calibration candidate removes the
  reproduced high-speed lateral blocker on this straight sample. It does not
  promote physical mapping to the default: the run used an explicit override
  and still requires calibration portability and five-P0 no-regression.
- The first calibrated dynamic run exposed a lifecycle mismatch: waiting for
  ego to reach `18m/s` consumed about half the short road before deferred
  target spawn, so the `18s` profile stopped at road end after only `6.35s`.
  The dynamic profile now waits only for sustained Apollo motion, then restores
  the canonical ego start pose once, clears old motion, materializes the
  declared initial speed, and spawns the target before starting scene time.
  Online run
  `runs/phase1_online_pairs/phase1_p0_lead_decel_accel_scene_reset_v4_20260711`
  restored pose with about `0.015m` error and executed the full lead profile
  from route_s about `22m` to `300.59m` in `17.65s`; actor and obstacle-GT
  contracts pass, and both backend rows are evaluable/comparable.
- v-t-gap now consumes only the common target-trace window. This removed the
  false negative gap generated by pairing pre-scene warmup ego poses with the
  deferred target's first row. The valid Apollo scene window has minimum gap
  about `15.11m`, maximum gap about `106.60m`, final gap about `92.12m`, zero
  collision/lane invasion, and Phase 1 status `degraded / large_final_gap`.
  Dynamic lifecycle and evidence alignment are therefore no longer the main
  blocker; Apollo longitudinal obstacle response is. This is not
  lead-decel-accel success or natural-driving evidence.
- Prediction-trigger pacing is now explicit: localization/chassis remain at
  20 Hz while GT obstacles trigger native Prediction/Planning at 10 Hz. In the
  stitcher diagnostic this reduced early sub-80ms Planning cycles from 225 to
  29, time-before-first-point replans from 41 to 5, fallback from 10 to 1, and
  lane invasions from 6 to 2. The no-stitch 10 Hz repeat retained zero
  collision/lane-invasion and slightly increased distance from 101.7m to
  107.6m, so the pacing change is not stitcher-only behavior tuning.
- A steering-only calibrated mapping diagnostic isolated the remaining
  straight-lane oscillation from Planning geometry. With the same 10 Hz
  Prediction pacing and trajectory stitching, route distance improved from
  187.9m to 217.1m, completion from 81.7% to 94.4%, lane invasions from 2 to
  0, CTE p95 from about 0.892m to 0.030m, and heading-error p95 from about
  0.147rad to 0.0053rad. The corresponding calibrated no-stitch run reduced
  CTE p95 from about 0.798m to 0.158m but still covered only about 106.5m,
  proving that calibrated steering and trajectory continuity close different
  failure mechanisms.
- A second clean no-guard `lane_keep_097` repeat reached about 218.6m / 95.0%
  with zero collision and lane invasion, CTE p95 about 0.174m, and heading
  error p95 about 0.0104rad. This closes the single-route repeat check but not
  calibration portability or the five-P0 matrix.
- The first full-window calibrated `curve217` run reached the scenario goal,
  then continued beyond the declared route because the legacy harness only
  had a follow-stop success condition. The extra post-goal motion produced
  four lane invasions and a false curve failure. The harness now terminates
  no-lead lane/curve cases only when route progress is at least 95% and ego is
  within the declared goal tolerance. The same online configuration then
  produced `route_completion_ratio=0.9922`, zero collision/lane invasion,
  reference-line contract `pass`, control-health `warn`, and Phase 1
  `success` in
  `runs/phase1_online_pairs/phase1_curve217_route_completion_fix_20260711`.
  The route metrics are recorded in the run's
  `analysis/route_health/route_health.json`.
  This is evidence for the scenario termination contract and this calibrated
  curve sample; it is not a natural-driving claim.
- This is a diagnostic candidate, not default physical-mapping promotion. It
  uses a local calibration artifact; junction and full five-P0 no-regression
  gates remain outstanding. The tracked physical
  candidate config now extends the current reference profile, disables all
  lateral guards, forbids silent legacy fallback, and maps only steering.

- The unfilled-map junction baseline
  `runs/longitudinal_unified_signed_stitcher_031_20260711/junction031_unified_signed_stitcher_no_guard__town01_rh_spawn031_goal056`
  reproduced 13 Piecewise Jerk optimizer infeasibilities and 13 speed
  fallbacks. Its shortest Planning trajectory was 72 points over 3.2 seconds,
  with terminal speed falling to about 0.047 m/s.
- Source inspection and map audit found 202 top-level lane blocks in the
  Town01 Apollo `base_map.txt`, but only 52 explicit `speed_limit` fields.
  Missing connector-lane fields reached Planning as zero-valued limits and
  were floored to a 0.1 m/s speed bound, making the optimizer constraints
  infeasible at the current vehicle speed.
- The clean-map repeat
  `runs/longitudinal_map_speed_contract_cleanmap_031_20260711/junction031_cleanmap_inherit_signed_stitcher__town01_rh_spawn031_goal056`
  restored the original map, filled only the 150 missing fields by inheriting
  the existing 11.176 m/s map limit, and preserved all 52 explicit limits.
  Optimizer infeasibility, speed fallback, zero speed-limit rows, and 0.1 m/s
  bound rows all fell to zero. The shortest trajectory returned to 111 points
  over 7.1 seconds, terminal speed stayed above about 3.06 m/s, and route
  completion reached about 83.6% without collision or lane invasion.
- The P0 static follow-stop pair
  `runs/phase1_online_pairs/phase1_p0_follow_stop_static_sidecar_pair_20260711`
  is now evaluable for both backends. The Apollo row uses the fixed-scene
  player for the stopped lead, records `legacy_controller_applied=false`,
  receives/applies Apollo control, and stops from an initial 295.1m bumper gap
  at about 6.26m with final ego speed about 0.054m/s and zero collision/lane
  invasion. Routing succeeded once, 579 of 660 Planning messages were
  non-empty, and fixed-scene, obstacle-GT, and v-t-gap reports pass. The pair
  comparison is `comparable` / `apollo_vs_planning_control_evaluable`.
- The same run exposed stale summary precedence: `summary.json` retained
  `control_handoff_status=control_missing` even though
  `analysis/apollo_control_handoff/apollo_control_handoff_report.json` was a
  non-blocking warn with more than ten thousand bridge control receives. The
  control-health analyzer now allows that stronger artifact to override the
  stale value, changing this run from false `fail` to `warn`. The control
  runtime overlay remains a diagnostic condition, so this is Phase 1 scenario
  evidence rather than default-backend or natural-driving promotion.
- The P0 dynamic lead-speed pair
  `runs/phase1_online_pairs/phase1_p0_lead_decel_accel_preroll_20260711`
  now has an explicit Apollo pre-roll and storyboard-owned termination. The
  Apollo fixed-scene trace starts after a 12s diagnostic warm-up, contains 352
  native rows, and completes the 70-to-40-to-70kph lead profile at scene time
  17.55s. This removes the previous 65s post-scene tail that made a dynamic
  run look like a successful static stop.
- The corrected pair is evaluable but Apollo is `degraded`, not successful:
  after scenario activation ego speed reaches about 6.82m/s at 1s and then
  remains roughly 5-7m/s while the lead executes its configured speed profile;
  the final gap is about 184m. Routing succeeds, Planning is materialized,
  control handoff is non-blocking warn, control health is warn, and there are
  no collisions or lane invasions. The next capability blocker is therefore
  the Apollo Planning longitudinal speed reference in the valid scenario
  window, not fixed-scene playback or CARLA control application. The pre-roll
  remains diagnostic and is not natural-driving evidence.
- The straight lateral regression run
  `runs/longitudinal_map_speed_contract_fix_lateral_regression_20260711/lane_keep097_map_speed_complete_unified_stitcher__town01_rh_spawn097_goal046`
  recorded no collision or lane invasion, cross-track-error p95 about 0.144 m,
  and heading-error p95 about 0.0153 rad. This supports that the map-contract
  repair did not regress the frozen straight-lane lateral behavior.
- A frozen-stitcher curve observation remained laterally bounded over its
  observed 214.7 m, but did not reach the final turn. A separate stitcher-on
  full-curve run still invaded a lane near route_s 237.6 m. Full curve
  completion therefore remains unproven, and trajectory stitching stays off
  in the frozen mainline configuration.
- Residual longitudinal-difference replans in the clean-map repeat generated
  full 111-point NORMAL trajectories immediately. They remain diagnostic but
  are not the reproduced fallback/short-trajectory mechanism.

Current P0 delivery and behavior blockers:

- Runtime delivery for the five-P0 comparison surface is no longer the active
  blocker after the fresh online matrix
  `runs/phase1_p0_matrix/cycle_inf_mainline_vehicle_param_map_speed_20260714`.
  Its
  top-level manifest reports `comparable_pair_count=5`,
  `invalid_pair_count=0`, and
  `comparison_target_status_counts={"apollo_vs_planning_control_evaluable": 5}`.
  PlanningControlBackend is `success` in all five rows. ApolloBackend is
  successful in the two Town01 route rows and behavior-failed in the three
  Baguang rows: `follow_stop_static` fails with `lane_invasion`, while
  `lead_decel_accel` and `cut_in_simple` fail with `collision`. The Town01
  curve row remains diagnostic because its route is a terminal-turn candidate.
  This upgrades Phase 1 comparison evidence and Apollo reference-runtime
  coverage, not Apollo natural-driving capability.
- Accepted Apollo P0 runs must continue to record clean assist evidence. Any
  run with `blocking_assists`, such as historical `legacy_followstop`, cannot
  support Apollo ego-ownership evidence.
- The five P0 rows are `follow_stop_static`, `lead_decel_accel`,
  `cut_in_simple`, `lane_keep_straight`, and `lane_keep_curve`.
- The `lead_decel_accel` P0 row now points to
  `configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml`, whose speed
  profile explicitly includes 70kph hold, deceleration to 40kph, a short hold,
  and acceleration back to 70kph. The older
  `lead_decel_70_to_40_20m.yaml` remains available as a deceleration-only
  diagnostic scene, but it is no longer the default P0 matrix row.
- Online semantic-check sample
  `runs/phase1_online_pairs/phase1_lead_decel_accel_semantic_online_20260626_004017`
  used the new combined `lead_decel_accel` scene. PlanningControlBackend ran to
  `phase1_status=success` with `artifact_completeness=pass`. ApolloBackend
  still timed out through the compatibility sidecar path; before the postprocess
  fix it was `invalid/missing_required_artifact` because `analysis/v_t_gap` was
  not generated. Running the now-required `tools/extract_v_t_gap.py` step on the
  same Apollo run produced `v_t_gap.status=warn` with 322 rows and reclassified
  the run as evaluable `failed/lane_invasion`. This is progress on the evidence
  boundary, not Apollo behavior success; the next new online run must generate
  v-t-gap before artifact-completeness without manual repair.
- P1 rows such as `cut_out_simple`, `junction_turn_no_signal`, and
  `lead_hard_brake` are Phase 1.1 backlog and should not block Phase 1 Core.
- The next code path to exercise is the unified
  `python3 -m carla_testbed phase1 run-pair --scenario <case> --out <dir>`
  entry. Behavior failure is acceptable evidence when the run is evaluable;
  setup/config/artifact `invalid` is not.
- The earlier full five-P0 matrix
  `runs/phase1_p0_matrix/phase1_p0_auto_artifacts_20260625_153515` proved the
  automatic artifact-normalization path but still carried timeout-era Town01
  Apollo rows. The 2026-07-01 matrix then moved those rows to evaluable
  route-health failures. The current 2026-07-14 matrix supersedes both for
  matrix-level status: it keeps all five pairs comparable, preserves
  PlanningControlBackend success in all rows, and records Apollo success in
  both Town01 route rows while retaining three explicit Baguang behavior
  failures. This is still not a natural-driving claim.
- Follow-up route-start alignment refresh on the current matrix now consumes
  `artifacts/startup_geometry_summary.json` instead of reusing runtime
  placeholder reports with
  `blocking_reasons=["route_start_runtime_pose_missing"]`. On the same matrix,
  `lane_keep_straight` reports static/initial/startup lateral-start evidence
  around `0.509m`, and `lane_keep_curve` reports about `0.318m`. Both rows are
  still `warn/spawn_lateral_offset_high`; this narrows the next Town01 Apollo
  action to spawn/route-start/reference-line alignment evidence before control
  tuning. It does not make Apollo lane keeping pass.
- Route-start probe
  `runs/phase1_online_pairs/phase1_lane_keep097_route_start_probe_20260701_033622`
  used the unified pair entry and passed
  `--apollo-override scenario.route_health.ego_offset_y_m=0.5089201844029378`
  only to the Apollo launch plan. PlanningControlBackend stayed `success`.
  Apollo remained comparable but failed with `route_health_failed` /
  `LANE_INVASION`; however, its refreshed route-start metrics changed from the
  previous roughly `0.509m` initial/startup offset to
  `initial_cross_track_error_m≈0.000000004` and
  `startup_geometry_localization_to_final_start_distance_m≈0.00019`. This
  proves the probe can remove the start-offset confounder for this row, but it
  also proves that start-offset correction alone is not a behavior fix. The
  route-start analyzer now treats this pattern as
  `spawn_lateral_offset_compensated_by_runtime_start_alignment` for its
  recommendation path, so postprocess no longer tells operators to repeat the
  same offset probe after initial/startup alignment is already low. The next
  Town01 Apollo work should stay on route/reference-line/lateral semantics and
  Planning debug export evidence, not control smoothing, PID, steer scale, or
  CARLA actuation.
- The same postprocess refresh now promotes Apollo timing diagnostics needed by
  channel cadence analysis (`publish_gap_trace.jsonl`,
  `topic_publish_stats.jsonl`, `carla_tick_health.jsonl`, and
  `carla_tick_health_summary.json`) into the canonical root artifact surface.
  Re-running channel-cadence diagnosis on the route-start probe removes the
  earlier `missing_publish_gap_trace` ambiguity and keeps a real blocker:
  `localization:isolated_header_sim_gap_over_contract` with publish-loop
  overrun / stale-sample skips and max tick stage `wall_time_pacing_sleep`.
  This is evidence for a GT publish cadence / pacing-queue issue to inspect
  before control tuning; it is not Apollo behavior success.
- The next bridge-side minimum fix keeps periodic `cyber_bridge_stats.json`
  visible to wrappers but makes the periodic write lightweight: it no longer
  flushes artifact buffers, health summaries, startup geometry summaries,
  planning summaries, or node artifacts from the 20Hz GT publish hot path.
  Full stats/summaries are still written at shutdown, and full/lightweight
  writes are counted and timed separately under
  `cyber_bridge_stats.artifact_buffering`
  (`full_stats_write_count`, `lightweight_stats_write_count`,
  `full_stats_write_duration_s_max`, and
  `lightweight_stats_write_duration_s_max`). Fresh online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_cadence_lightweight_verify2_20260701_043727`
  confirms those fields are now materialized
  (`full_stats_write_count=1`, `lightweight_stats_write_count=16`,
  `full_stats_write_duration_s_max≈0.0055s`,
  `lightweight_stats_write_duration_s_max≈0.0818s`). The same run still
  finalizes as evaluable Apollo `failed/route_health_failed`, and channel
  cadence remains a non-blocking `warn` with publish-loop overruns and
  stale-sample skips (`over_target_period_count=85`,
  `gt_stale_sample_skip_count=60`). This proves the stats hot-path
  instrumentation is present, not that Apollo behavior improved.
- The same `phase1_lane_keep097_cadence_lightweight_verify2_20260701_043727`
  sample also exposed a root-artifact gap: the nested compatibility run had an
  `apollo_reference_line_contract_report.json`, but the Phase 1 root did not.
  Artifact normalization now promotes that existing nested report without
  rewriting its status. After promotion, `apollo_link_health` moved the primary
  blocker upstream to reference-line heading semantics; the latest re-analysis
  below further refines that blocker to a PATH_FALLBACK trajectory-vs-segment
  heading mismatch. The lateral semantics report remains supporting diagnostic evidence with
  `route_lateral_error_opposes_simple_lat_lateral_error`. Guard-related
  raw-to-mapped steering changes are now surfaced as
  `control_mapping_apply:bridge_guard_intervention` secondary evidence
  (`guard_apply_count=77`, `guard_apply_ratio≈0.0418`) instead of being
  reported as generic bridge mapping failure. The next validation should
  inspect Apollo reference-line, routing lane IDs, HDMap projection, and the
  CARLA route lateral-error / Apollo `simple_lat` convention on the same
  `route_s` window before changing steer scale, PID, smoothing, or actuation
  mapping.
- Follow-up normalization on the same sample now promotes the associated
  reference-line raw debug inputs and Apollo HDMap projection surface into the
  Phase 1 root as well:
  `artifacts/apollo_reference_line_contract.jsonl`,
  `artifacts/planning_topic_debug.jsonl`,
  `artifacts/planning_topic_debug_summary.json`,
  `artifacts/planning_route_segment_debug.jsonl`,
  `artifacts/apollo_route_segment_debug.jsonl`,
  `artifacts/apollo_planning.INFO`,
  `artifacts/control_trajectory_consume_debug.jsonl`,
  `analysis/planning_materialization/planning_materialization_report.json`,
  `artifacts/apollo_hdmap_projection.jsonl`, and
  `analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json`.
  Re-running Phase 1 postprocess on the root then regenerates
  `apollo_reference_line_contract_report.json` from canonical root inputs. The
  HDMap projection state is now correctly reported as
  `file_present=true`, `artifact_status=artifact_empty`, and
  `empty_reason=apollo_hdmap_projection_artifact_empty_no_exported_rows`
  instead of `artifact_missing`. This is better evidence hygiene, not a
  behavior fix: `apollo_link_health.primary_blocker` remained in the
  reference-line heading family at this stage. The next
  highest-value validation is to make the existing HDMap projection exporter
  produce official `source=apollo_hdmap_api` rows for the same run, then
  re-check reference-line heading/lateral metrics before treating bridge guard
  intervention or steering actuation as primary.
- The next offline projection probe on that same run successfully invoked the
  existing `tools/export_apollo_hdmap_projection.py` path against Apollo
  `map_xysl` in `apollo_neo_dev_10.0.0_pkg`. It produced official
  `source=apollo_hdmap_api` rows on lane `15_1_1`; the initial samples have
  near-zero lateral/heading error, while later runtime ego samples show high
  projection error after the vehicle has already drifted
  (`runtime_ego_projection.status=fail`,
  `heading_error_p95_rad≈0.89`, `lateral_error_p95_m≈1.00`). The projection
  analyzer now classifies ego-only projection as
  `claim_evidence_scope=runtime_ego_only`,
  `status=insufficient_data`, with
  `apollo_hdmap_projection_static_route_samples_missing`; it does not turn
  runtime drift into a static HDMap/localization contract failure. After
  postprocess, `apollo_reference_line_contract` is still `fail`; at this stage
  its only blocking reason is the still-generic
  `planning_reference_heading_error_high`.
- Follow-up normalization then promoted the nested `route.json`, allowing the
  same exporter to run with `--include-route-samples --include-start-goal`.
  The refreshed projection report now has official static route/start/goal
  evidence: `status=pass`, `claim_grade=true`,
  `claim_evidence_scope=route_start_goal`,
  `nearest_lane_id_topk=["15_1_1"]`, static route heading p95 about
  `0.00013rad`, and static lateral p95 `0.0m`. This narrows the latest
  Town01 `lane_keep_097` blocker again: it is no longer map identity or static
  HDMap projection for this sample. Re-running the reference-line contract with
  trajectory-vs-segment heading metrics shows the remaining blocker is
  `planning_reference_line:planning_path_fallback_segment_heading_mismatch`:
  normal trajectory-vs-segment heading p95 is about `2.9e-6rad`, but
  PATH_FALLBACK trajectory-vs-segment heading p95 is about `3.14159rad`.
  Follow-up online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_lookahead_heading_probe_20260701_053934`
  confirms the bridge now records future/non-expired lookahead trajectory
  heading diagnostics. The run remains evaluable Apollo
  `failed/route_health_failed`, and `phase1_status.primary_behavior_blocker`
  is still `planning_path_fallback_segment_heading_mismatch`. The short-segment
  PATH_FALLBACK future heading p95 is about `3.14159rad`; the more robust
  lookahead heading p95 is also about `3.14159rad`, so the mismatch is no
  longer just an expired-prefix/one-step sampling artifact. A manual exporter
  rerun on the same Apollo run then generated `238` official
  `source=apollo_hdmap_api` rows and refreshed
  `apollo_hdmap_projection.status=pass` / `claim_grade=true`, with about
  `231.4m` projection-s coverage on lanes `15_1_1` / `15_1_-1`. This confirms
  the immediate blocker is not static HDMap projection; the remaining
  automation gap was that Phase 1 postprocess did not yet invoke the exporter
  when the run-local projection file was empty. The next validation should
  confirm automatic projection export in postprocess and then inspect the
  PATH_FALLBACK onset window, Planning trajectory sample source,
  route-segment heading export, and Apollo Planning logs before changing
  control mapping, steer scale, PID, or actuation.
- Follow-up online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_auto_projection_verify_20260701_060342`
  confirms that Phase 1 postprocess now invokes the HDMap projection exporter
  when the run-local projection artifact is empty. The Apollo run remains an
  evaluable `failed/route_health_failed` sample with
  `primary_behavior_blocker=planning_path_fallback_segment_heading_mismatch`,
  but `analysis/apollo_hdmap_projection_export/apollo_hdmap_projection_export_status.json`
  records `status=pass`, `row_count=59`, and `ok_row_count=59`. The refreshed
  projection report is `status=pass` / `claim_grade=true` with
  `claim_evidence_scope=route_start_goal`, about `231.4m` projection-s
  coverage, `nearest_lane_id_topk=["15_1_1"]`, heading p95 about
  `0.00013rad`, and lateral p95 `0.0m`. This removes the automatic
  projection-export gap for this row. The remaining blocker is still upstream
  Planning reference-line / PATH_FALLBACK trajectory semantics, not static map
  projection and not control actuation. Re-analysis after the fallback
  lookahead diagnostic patch keeps the same fail verdict and adds that
  PATH_FALLBACK future lookahead p95 distance is only about `0.397m`, the
  minimum lookahead distance is effectively zero, all fallback lookahead
  distances are below `1m`, and about `70.8%` of fallback samples have
  first-nonexpired theta vs future-lookahead heading near the reverse-heading
  family. This makes the next target Planning fallback trajectory point order /
  heading semantics, not HDMap projection wiring. The bridge planning debug
  helper now records `trajectory_future_window_points` around the first
  non-expired and lookahead indices so the next online run can inspect the
  local PATH_FALLBACK point order directly instead of inferring it only from
  aggregate heading deltas.
- Follow-up online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_future_window_behavior_verify_20260701_065329`
  used the same `apollo_cyberrt_town01_behavior_recovery` platform and
  confirmed the new local trajectory window evidence lands in real Apollo
  artifacts. The pair is still `comparison_status=comparable` /
  `comparison_target_status=apollo_vs_planning_control_evaluable`.
  PlanningControlBackend is `success`. ApolloBackend remains an evaluable
  `failed/route_health_failed` row with
  `primary_behavior_blocker=planning_path_fallback_segment_heading_mismatch`
  and `behavior_blocker_layer=planning_reference_line`. In the Apollo
  `artifacts/planning_topic_debug.jsonl`, there are `93` NORMAL rows and `194`
  PATH_FALLBACK rows; all `194` PATH_FALLBACK rows now carry
  `trajectory_future_window_points`. The refreshed reference-line contract is
  still `fail`; PATH_FALLBACK first-nonexpired theta vs future-lookahead
  heading p95 is about `3.14159rad`, future-lookahead distance p95 is about
  `0.419m`, all fallback lookahead distances are below `1m`, and the
  reverse-heading ratio is about `0.792`. Example local windows show jumps from
  `y≈-247.93` back to `y≈-248.28/-248.52` while point theta remains around
  `+1.5707rad`, making Planning fallback trajectory point order / heading
  semantics the next highest-value Apollo blocker.
  A follow-up log/timeseries inspection further narrows the failure chain:
  before the reverse-looking PATH_FALLBACK windows dominate, route-local
  cross-track error grows from about `-0.76m` at `sim_time≈98.54s` to about
  `-2.57m` at `sim_time≈99.74s`, while Apollo Planning logs report
  `Decide path bound failed`, `piecewise jerk path optimizer failed`, and
  `PATH_DECIDER` failure. The fallback debug lines show path bounds around
  `[-0.945m, 0.945m]` but `self_opt_l≈1.598m`. The current blocker should
  therefore be read as a Planning path-bound / lateral-state infeasibility
  feeding PATH_FALLBACK, then local fallback point-order/heading inconsistency.
  The reference-line analyzer now parses this Planning INFO path-bound family
  into `planning_info_log_path_bound_evidence`, and `apollo_link_health`
  surfaces the next validation as reference-line/path-bound lateral envelope
  investigation. Do not tune steering/control mapping or add smoothing until
  this reference-line/path-bound failure is explained.
  Control evidence around the same window adds an important boundary: sampled
  `control_apply_trace.jsonl` rows show `apollo_steer_raw=-1.0` before and
  during the lateral drift, while bridge mapping initially limits it to
  `-0.35` and later applies `-1.0`. The bridge guard/limit is still a
  confounder, but the next root-cause loop should inspect Apollo source-control
  lateral semantics and path-bound corridor alignment together, not treat the
  event as CARLA actuation alone.
- No-guard diagnostic probe
  `runs/phase1_online_pairs/phase1_lane_keep097_no_lateral_guard_probe_20260701_072248`
  closed the main guard-vs-source-control ambiguity. The pair stayed
  comparable: PlanningControlBackend succeeded, while ApolloBackend remained
  evaluable `failed/route_health_failed` with the same
  `primary_behavior_blocker=planning_path_fallback_segment_heading_mismatch`.
  The resolved Apollo config confirms
  `low_speed_steer_guard_enabled=false`,
  `low_speed_sustained_saturation_guard_enabled=false`,
  `sustained_lateral_guard_enabled=false`, and
  `trajectory_contract_lateral_guard_enabled=false`; bridge stats report
  `lateral_guard_apply_count=0` and
  `trajectory_contract_lateral_guard_apply_count=0`. With guards disabled,
  `control_attribution` changes from `bridge_guard_intervention` to
  `source_control_semantics`, and `apollo_lateral_semantics` changes from a
  control-mapping suspicion to `reference_line_semantics`. Apollo raw steer
  still saturates early (`first_raw_sat` around `sim_time≈94.28s`,
  `route_s≈0.65m`, `cross_track_error≈-0.58m`), and the run reaches
  `|cross_track_error|≥2m` by `sim_time≈95.33s`. This shows the guard was a
  confounder/limiter, not the primary source of the failure. The refreshed
  `apollo_lateral_semantics` report now adds `critical_window_samples` and can
  read nested wrapper-run `control_decode_debug.jsonl` as a fallback evidence
  source. On this no-guard run, the critical anchor is around
  `sim_time≈94.48s` / `route_s≈0.85m`: route CTE is about `-0.70m`, Apollo
  `simple_lat` lateral error is about `+0.24m`, and `apollo_steer_raw=-1.0`.
  Thus source steer is same-sign with route CTE but opposite-sign with Apollo
  `simple_lat` in the same window. The same report now carries
  `reference_line_context`: first PATH_FALLBACK onset follows about `1.4s`
  later, `path_fallback_trajectory_ratio≈0.73`,
  `path_fallback_future_lookahead_reverse_heading_ratio≈0.65`, and Planning
  INFO path-bound evidence classifies the fallback as
  `fallback_lateral_state_outside_path_bound` with `241` out-of-bounds
  fallback samples. The first logged fallback path-bound violation is at
  Apollo Planning `s≈50.215m`: `self_opt_l≈1.367m` exceeds the upper bound
  `0.945m` by about `0.422m`. The next experiment should inspect Apollo Planning
  path-bound corridor / route-lateral frame semantics, not repeat a guard-only
  probe.
- Follow-up blocker surfacing on `2026-06-26` keeps the same verdicts but makes
  the next Baguang action explicit at the matrix/operator layer. Recomputing
  the three Baguang pair comparisons with non-hidden run directories preserves
  `comparison_status=comparable` and
  `comparison_target_status=apollo_vs_planning_control_evaluable`, while each
  Apollo row now exposes
  `primary_behavior_blocker=lane_departure_with_route_simple_lat_sign_convention_candidate`
  and `behavior_blocker_layer=apollo_lateral_semantics`. This does not turn
  `failed/lane_invasion` into success. It narrows the next fix to Apollo
  lateral semantics / route-lateral sign convention / Planning reference-line
  materialization evidence, and explicitly says not to tune steer scale,
  smoothing, or control gains first.
- The same blocker-surfacing pass now narrows the two Town01 Apollo route-only
  rows from generic `failed/timeout` to
  `primary_behavior_blocker=routing_available_planning_missing_timeout` and
  `behavior_blocker_layer=apollo_planning_materialization`. Reclassifying
  `lane_keep_straight` and `lane_keep_curve` from
  `runs/phase1_p0_matrix/phase1_p0_auto_artifacts_20260625_153515` preserves
  `status=failed` and `failure_reason=timeout`, but their progress logs show
  `routing=1`, `planning=0`, `control_tx=0`; their `timeseries.csv` rows show
  essentially zero `route_s` progress and final ego speed `0.0 m/s`. This is
  evidence to inspect Apollo Planning readiness/reference-line materialization
  and route/lane compatibility before control mapping or actuation tuning. It
  is not Apollo lane-keep success and does not relax the timeout gate.
- PlanningControlBackend route-only follow-up found that the builtin launch
  plan was applying the generic `simple_acc_route_follower` 70kph algorithm
  speed to Town01 lane/curve diagnostics. The P0 route-only scenarios now
  declare conservative diagnostic target speeds in `success_intent`
  (`lane_keep_097=10.0m/s`, `curve217=8.0m/s`), and the `carla_builtin` launch
  plan prefers those scenario values over the algorithm default. This is a
  baseline playback consistency fix, not an Apollo capability claim; the next
  online P0 matrix must prove whether the PlanningControl route-only
  lane/collision failures are actually reduced.
- Online validation
  `runs/phase1_online_planning_control_route_speed_20260626_011047` then ran
  both route-only PlanningControlBackend samples directly against CARLA Town01.
  `lane_keep_097` and `curve217_diagnostic` both produced
  `phase1_status=success`, `artifact_completeness=pass`, `collision_count=0`,
  and `lane_invasion_count=0`. This proves the standalone
  PlanningControlBackend speed-surface fix for those two route-only samples; it
  does not change Apollo behavior, and it still needs a full P0 matrix rerun to
  replace the older matrix rows.
- Unified pair validation
  `runs/phase1_online_pairs/phase1_lane_keep097_unified_speedcheck_20260626_011636`
  then exercised the real `phase1 run-pair` entry for the same Town01 straight
  row. The PlanningControlBackend launch command includes
  `--target-speed-mps 10.0` and its run is `phase1_status=success`,
  `artifact_completeness=pass`, `collision_count=0`, and
  `lane_invasion_count=0`. The ApolloBackend run remains evaluable
  `failed/timeout`, but the blocker is now more precise:
  `primary_behavior_blocker=routing_missing_timeout`,
  `behavior_blocker_layer=apollo_routing_materialization`, with progress logs
  showing `routing=0`, `planning=0`, `control_tx=0`, and `last_phase=routing_wait`.
  The refreshed comparison is still `comparison_status=comparable` and
  `comparison_target_status=apollo_vs_planning_control_evaluable`. This moves
  the next Apollo Town01 action earlier than Planning: inspect routing request
  send/materialization, route goal validity, map/route compatibility, and
  Dreamview/Apollo routing status before debugging Planning, Control, or
  actuation.
- Deeper inspection of that Apollo run then found an outer Phase 1 timeout
  mismatch rather than enough evidence for a route-materialization root cause:
  the nested `artifacts/command_materialization_summary.json` still reported
  `last_blocking_reason=apollo_startup_warmup`, `first_eligible_ts_sec` near
  `88.9s`, no routing request sent yet, and roughly `1.8s` warmup remaining
  when the Phase 1 runner killed the process. The same runtime stdout declares
  `timing_budget_total_upper_bound_sec: 215.0`, while the unified pair was
  launched with `--timeout-s 180`. The executor now preserves the requested
  timeout but raises Apollo compatibility runs to a recorded
  `effective_runtime_timeout_s=240.0`, leaving PlanningControlBackend at the
  requested timeout. This is timeout-budget alignment and provenance; it does
  not make Apollo behavior successful. The next online validation is to rerun
  the same lane-keep pair and check whether Apollo gets past startup warmup to
  an actual routing/planning/control blocker.
- Online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_timeout_policy_20260626_013215`
  then reran the same pair through the unified entry with user
  `--timeout-s 180`. The PlanningControlBackend row remained
  `phase1_status=success`, while the ApolloBackend row recorded
  `requested_runtime_timeout_s=180.0`, `effective_runtime_timeout_s=240.0`,
  and ran for about `247.6s` before timeout. This proves the timeout policy is
  applied and shows the previous 180s cutoff was too short for this
  compatibility path. The new Apollo artifacts narrow the blocker again:
  `routing_request_count=1`, `routing_success_count=1`, Planning materialized
  non-empty trajectories (`nonempty_trajectory_count=198`), and
  `/apollo/control` remains absent (`control_tx_count=0`). Refreshed Phase 1
  status now records
  `primary_behavior_blocker=planning_available_control_process_crash_timeout`
  at `behavior_blocker_layer=apollo_control_process_health`, because the
  deferred Control mainboard log contains
  `tcmalloc.cc:333] Attempt to free invalid pointer` and the process survives
  at 5s but not 10s. Planning debug still reports exported
  `reference_line_nonempty_ratio=0.0`, so reference-line export remains a
  secondary diagnostic gap, but the next fix should inspect Apollo Control
  runtime process health / runtime overlay provenance before control mapping,
  steering scale, smoothing, or actuation work.
- Online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_control_bvarfix_20260626_020756`
  then tested the narrow `deferred_control_disable_bvar_dump=true` hypothesis.
  The Apollo start log confirms `APOLLO_DISABLE_BVAR_DUMP=1`, routing still
  succeeds, and Planning again emits non-empty trajectories
  (`nonempty_trajectory_count=193`), but Control still crashes with the same
  `tcmalloc.cc:333] Attempt to free invalid pointer` signature before
  `/apollo/control` materializes. This makes bvar dumping an excluded
  first-order cause for the Town01 route-only sample. The canonical
  `town01_apollo_route_health` config keeps
  `deferred_control_disable_bvar_dump=false`; changing that flag is not a
  behavior fix.
- Online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_explicit_config_20260626_024805`
  then confirmed the actual Phase 1 launch command records
  `--config configs/io/examples/town01_apollo_route_health.yaml` and applies
  the same `requested_runtime_timeout_s=180.0` to
  `effective_runtime_timeout_s=240.0` Apollo policy. PlanningControlBackend
  again succeeded with `collision_count=0` and `lane_invasion_count=0`. Apollo
  remained an evaluable `failed/timeout`; after the executor refresh-order fix,
  its `phase1_status.json` records
  `primary_behavior_blocker=planning_available_control_process_crash_timeout`,
  Planning non-empty trajectories (`planning_nonempty_trajectory_count=190`),
  `control_tx_count=0`, and the same `tcmalloc_invalid_free` Control mainboard
  crash. The pair comparison remains `comparable` /
  `apollo_vs_planning_control_evaluable`, with `blocking_assists=[]`.
- Diagnostic online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_behavior_recovery_diag_20260626_030455`
  then tried the explicit
  `configs/platforms/apollo_cyberrt_town01_behavior_recovery.yaml` profile.
  This is a `diagnostic_only` control-runtime survival probe, not a default
  backend change. PlanningControlBackend again succeeded, but the Apollo row is
  `invalid/no_timeseries`, not an evaluable behavior failure. Refreshed
  `phase1_status.json` now reports
  `primary_setup_blocker=apollo_startup_command_materialization_missing_timeout`
  at `setup_blocker_layer=apollo_runtime_startup_materialization`: the overlay
  manifest was active (`selected_count=17`), routing/prediction/planning
  processes were seen, no Control process was seen, `startup_stage` stopped at
  `adapter_prepare_done`, and no command-materialization/timeseries/control
  artifacts were produced before timeout. The pair comparison is therefore
  `partially_evaluable` with `missing_evaluable_apollo_reference_backend`.
  This narrows the next diagnostic-profile task to startup/materialization
  lifecycle, and it does not replace the default-profile Control crash blocker.
- Follow-up diagnostic validation
  `runs/phase1_online_pairs/phase1_lane_keep097_behavior_recovery_diag_dreamview_timeoutfix_20260626_034500`
  then bounded the Dreamview fallback start command and re-ran the same
  `apollo_cyberrt_town01_behavior_recovery` profile. This removed the
  startup/materialization blocker: `dreamview_launch.log` records fallback
  startup readiness, root/nested `timeseries.csv` are present, the comparison is
  now `comparable` / `apollo_vs_planning_control_evaluable`, and Apollo is an
  evaluable `failed/timeout` instead of `invalid/no_timeseries`. The refreshed
  Apollo `phase1_status.json` reports
  `primary_behavior_blocker=finalized_route_health_failure_platform_wrapper_timeout`
  at `behavior_blocker_layer=apollo_route_health_behavior`. Key evidence:
  routing succeeded (`routing_success_count=1`), Planning emitted non-empty
  trajectories (`planning_nonempty_trajectory_count=319`), `/apollo/control`
  materialized (`control_tx_count=2418`), Control survived, and the ego vehicle
  advanced about `99.76m` before ending near zero speed. The nested Apollo
  route-health summary is finalized. After fixing route-health state-timeline
  timestamp selection to prefer sim-time fields over wall-time `timestamp`,
  `route_establishment_latency_sec` is now `8.0s` and passes the `45s`
  threshold; the previous billion-second latency was a mixed-clock artifact.
  The remaining finalized route-health failure is
  `fail_reason=PLANNING_NONZERO_RATIO`,
  `route_health_label=route_established_but_behavior_unhealthy`,
  `route_distance_achieved_m≈99.76`, and
  `route_completion_ratio≈0.434`, with failure codes
  `PLANNING_NONZERO_RATIO`, `ROUTE_DISTANCE_ACHIEVED_M`, and
  `ROUTE_COMPLETION_RATIO`. The remaining blocker is therefore the finalized
  route-health behavior failure plus a platform-wrapper lifecycle timeout after
  finalized summary, not a missing Control process and not a proof of Apollo
  lane-keep success.
- Wrapper lifecycle follow-up
  `runs/phase1_online_pairs/phase1_lane_keep097_behavior_recovery_marker_20260626_040644`
  added a Phase 1 runtime completion marker for nested Town01 route-health
  `summary_status=finalized`. The same diagnostic profile remains
  `comparable` / `apollo_vs_planning_control_evaluable`, but Apollo is now
  `failed/route_health_failed` instead of wrapper `failed/timeout`:
  `platform_execution_result.dispatch.command.timed_out=false`,
  `completion_marker.id=town01_route_health_finalized_summary`, and
  `completion_marker.success=false`. The behavior blocker is now
  `primary_behavior_blocker=finalized_route_health_failure` at
  `behavior_blocker_layer=apollo_route_health_behavior`. Runtime evidence again
  shows materialized routing/planning/control (`routing_success_count=1`,
  `planning_nonempty_trajectory_count=314`, `control_tx_count=2404`) and ego
  route progress (`route_s_delta_m≈109.86`). A follow-up analyzer refresh now
  computes `planning_nonzero_ratio` on the post-route-established Planning
  window instead of counting pre-route zero-trajectory warmup messages; the
  check is `0.994` and passes, while preserving all-message counts for audit.
  Finalized route-health still fails `ROUTE_DISTANCE_ACHIEVED_M` and
  `ROUTE_COMPLETION_RATIO` (`route_completion_ratio≈0.478`). This closes the
  wrapper-finalization and Planning-ratio attribution blockers for this path;
  it does not make Apollo lane-keeping pass. The next useful fix is Apollo
  route-health behavior: why the ego covers only about 110m of a ~230m route
  before the acceptance window ends or the vehicle stops, including route target
  length/duration, path fallback/matched-point/reference-line evidence, and
  destination/longitudinal behavior.
- Follow-up online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_behavior_recovery_900ticks_20260626_042955`
  extended only the diagnostic behavior-recovery route-health window from 700
  to 900 ticks. This did not change acceptance thresholds or control mapping.
  The pair remains `comparable` /
  `apollo_vs_planning_control_evaluable`; PlanningControlBackend succeeds, and
  Apollo remains `failed/route_health_failed`. The longer window proves
  routing/planning/control continue to materialize (`routing_success_count=1`,
  Planning non-empty trajectories observed, `control_tx_count≈3341`) and the
  wrapper stops by finalized-summary marker rather than timeout. It also
  narrows the active behavior blocker: Apollo reaches about `121.8m`
  (`route_completion_ratio≈0.530`) but then hits `LANE_INVASION` around
  `route_s≈105-111m`, cross-track error grows past about `-1.0m`, and the ego
  later stops near `route_s≈121.8m` with Apollo commanding strong brake. This is
  not lane-keep success. A follow-up artifact-surface fix now promotes the
  nested control/config/bridge summaries to the Phase 1 root and overlays sparse
  `timeseries.csv` control placeholders from `artifacts/control_apply_trace.jsonl`.
  Re-running `tools/analyze_control_attribution.py --run-dir` on the same Apollo
  run gives `verdict.status=pass`,
  `control_chain_status=apollo_control_attributed`, `control_source=/apollo/control`,
  and `dominant_breakpoint=none`. That rules out a missing raw/mapped/applied
  control evidence chain for this sample. Running the existing
  `apollo_lateral_semantics` analyzer on the same run reports `status=warn`,
  `suspected_layer=target_point_semantics`, and the anomaly
  `high_lateral_drift_with_low_source_steer`: the route is near-straight,
  cross-track error grows above `1m`, while Apollo source/applied steer remains
  near zero. Because matched/target/kappa debug fields are still missing, this
  is diagnostic evidence, not a root-cause proof. The next useful fix is not
  more validation machinery and not steer-scale tuning; inspect lane-event
  timing, route/lane sign convention, path fallback/matched-point evidence, and
  why Apollo transitions from forward progress to a mid-route full stop.
- Inf-cycle online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_infcycle_20260630_235802`
  showed that the previous `240s` Apollo compatibility timeout floor was still
  too short for the current Town01 behavior-recovery path: Apollo reached
  `control_output` with `routing=1`, `planning=83`, and `control_tx=288` shortly
  before the wrapper killed the process. The Apollo compatibility floor is now
  `300s`. Follow-up run
  `runs/phase1_online_pairs/phase1_lane_keep097_timeout300_20260701_000507`
  validated that this changes the Apollo row from a platform timeout into an
  evaluable finalized behavior failure: root Phase 1 status is
  `failed/route_health_failed`, `dispatch.command.timed_out=false`, and the
  nested finalized summary reports `routing_request_count=1`,
  `routing_success_count=1`, `planning_message_count=270`, `control_tx_count=1621`,
  `control_handoff_status=control_consuming_with_nonzero_planning`,
  `exit_reason=LANE_INVASION`, `lane_invasion_count=4`, and
  `route_completion_ratio≈0.008`. This is a runtime-finalization improvement
  and a sharper Apollo behavior failure, not lane-keep success. The same
  pair's PlanningControlBackend row is `invalid/no_timeseries` because
  `run-pair` was launched without `--start-carla` and CARLA `load_world(Town01)`
  timed out before any timeseries was produced. That invalid row is setup /
  environment evidence, not a PlanningControlBackend behavior loss. The next
  clean pair validation should own CARLA explicitly with `--start-carla` or use
  a proven persistent CARLA session, then re-check whether the pair is
  comparable while preserving the Apollo `route_health_failed` behavior
  blocker.
- Clean pair online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_clean_owned_carla_20260701_002332`
  then reran the same Town01 straight-lane row with `--start-carla`. This
  closes the previous PlanningControl invalid row: CARLA session startup is
  recorded as `ready`, the PlanningControlBackend row completes in about `10s`,
  writes `timeseries.csv`, and reports `phase1_status=success`,
  `artifact_completeness=pass`, `collision_count=0`, and
  `lane_invasion_count=0`. The pair comparison is now `comparable` /
  `apollo_vs_planning_control_evaluable`. Apollo still fails, but now as a
  clean behavior failure rather than platform timeout:
  `phase1_status=failed`, `failure_reason=route_health_failed`,
  `primary_behavior_blocker=finalized_route_health_failure`,
  `behavior_blocker_layer=apollo_route_health_behavior`, and
  `dispatch.command.timed_out=false`. Nested Apollo evidence shows
  `routing_success_count=1`, `planning_message_count=301`,
  `messages_with_nonzero_trajectory_points=273`, `control_tx_count=1600`,
  `control_handoff_status=control_consuming_with_nonzero_planning`,
  `exit_reason=LANE_INVASION`, and `lane_invasion_count=3`. The next blocker is
  no longer Phase 1 pair comparability for this row; it is Apollo
  route-health/lateral behavior plus control/channel quality. A follow-up
  postprocess fix now lets route-only Apollo Phase 1 runs refresh the same
  control-handoff, control-attribution, control-health, lateral-semantics, and
  link-health reports that fixed-scene Apollo runs already generated. Re-running
  postprocess on this sample produces `apollo_control_handoff.status=warn`,
  `control_attribution.verdict.status=fail` with
  `dominant_breakpoint=bridge_mapping`, `control_health.status=insufficient_data`
  with `failure_reason=runtime_contract_missing_status`. A second evidence
  refresh fixed a channel-stats artifact sampling false positive: sampled
  `bridge_control_decode.jsonl` rows were no longer used as `/apollo/control`
  cadence truth when `cyber_bridge_stats.artifact_buffering` proves
  `control_debug_artifact_sample_stride=50`. The refreshed channel cadence now
  treats `/apollo/control` as counter-derived warn evidence
  (`message_count=1666`, `hz≈59.9`, non-promotion-grade), and link-health
  `primary_blocker` moves to
  `localization_gt_contract:heading_error_to_route_high`. This is sharper
  evidence than the previous missing-report/cadence false-positive gap, but it
  is still Apollo behavior/evidence failure, not lane-keep success.
- Follow-up online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_postprocess_fix_20260701_005244`
  confirms the same pattern on a fresh owned-CARLA pair: CARLA session startup
  and shutdown are clean, PlanningControlBackend is `success`, and Apollo is an
  evaluable `failed/route_health_failed` row rather than a timeout or startup
  failure. The Apollo nested route-health run materializes
  `routing_success_count=1`, non-empty Planning, and `/apollo/control`, then
  finalizes with `exit_reason=LANE_INVASION`, `route_completion_ratio≈0.010`,
  and `route_distance_achieved_m≈2.4`.
- The same run exposed a postprocess wiring gap: the launch plan previously
  ran the generic evidence bundle and link-health tools, but did not invoke the
  Phase 1 postprocess function that writes route-only Apollo
  `apollo_control_handoff`, `control_attribution`, `control_health`,
  `apollo_lateral_semantics`, and localization/link-health inputs. The
  launch plan now calls `tools/postprocess_phase1_run.py` before the generic
  analyze step and final link-health refresh. Replaying that postprocess on the
  fresh sample writes the missing run-local reports. The first refreshed blocker
  was `localization_gt_contract:heading_error_to_route_high`; a later context
  pass now shows this high full-run route-heading error appears after lateral
  departure, not before it. `/apollo/control` cadence remains sampled
  counter-derived warn evidence, not promotion-grade.
  A second small artifact-surface fix promotes the unique nested
  `analysis/route_health/` report set to the Phase 1 run root, so
  localization/link-health no longer misreport route-health as absent when the
  nested legacy runner already wrote it. `run_phase1_postprocess()` now performs
  that normalization itself before running per-layer analyzers, which keeps
  launch-plan postprocess consistent with manual replay. Replaying postprocess
  on the latest online sample after that fix removes the stale
  `timeseries_missing` and `route_health_missing` localization warnings. The
  localization contract now records
  `route_heading_error_after_lateral_departure_candidate`, with
  pre-departure route-heading p95 about `0.000198rad`, first lateral departure
  at `sim_time≈93.60s`, and first route-heading fail later at
  `sim_time≈94.05s`. That keeps localization non-claim-grade because other
  fields are still missing, but it no longer treats full-run
  `heading_error_to_route_high` as the primary root cause.
- The next inf-cycle pass fixed a diagnosis conflict rather than changing
  behavior. On
  `runs/phase1_online_pairs/phase1_lane_keep097_phase1post_auto_20260701_011535`,
  `control_attribution` proves raw->mapped and mapped->applied steer consistency
  (`error_p95=0.0` for both) while the lateral-semantics report had been
  promoting a duplicate `raw_mapped_applied_mismatch` as the primary suspected
  layer. `apollo_lateral_semantics` now consumes the existing
  `control_attribution_report.json` in run-dir mode and suppresses that specific
  mismatch when control attribution already marks both control mapping legs as
  pass. The refreshed report keeps
  `source_steer_same_sign_as_lateral_error`, moves `suspected_layer` to
  `reference_line_semantics`, and records the suppressed control-mapping
  anomaly for auditability. The next refresh connected the existing
  `artifacts/apollo_control_raw.jsonl` Apollo Control debug rows to
  lateral-semantics run-dir analysis. Because those rows carry Apollo
  `simple_lat` / target / matched-point fields but no explicit `sim_time`, the
  analyzer now infers `sim_time` from `control_header_timestamp_sec` anchors in
  `control_apply_trace.jsonl`; it still does not treat raw
  `steering_target` as normalized `apollo_steer_raw`. Replaying the analyzer on
  the same run raises `apollo_simple_lat_lateral_error_abs.count` to `102` and
  changes `apollo_link_health.primary_blocker` to
  `apollo_lateral_semantics:route_lateral_error_opposes_simple_lat_lateral_error`.
  Generic environment, bridge, localization, HDMap/reference-line, route
  establishment, module consumption, control-health, perception, prediction,
  assist, and natural-driving artifact gaps remain secondary blockers. This
  narrows the next Apollo work to route-lateral field sign convention,
  official projection/reference-line pairing, and Control simple_lat semantics
  before lane invasion; it is not a lane-keep success and does not relax any
  Phase 1 gate. Re-running `tools/postprocess_phase1_run.py` and
  `tools/compare_scenario_runs.py` on the same pair preserves
  `comparison_status=comparable` and Apollo `failed/route_health_failed`, but
  updates the Apollo row to
  `primary_behavior_blocker=route_lateral_error_opposes_simple_lat_lateral_error`
  and `behavior_blocker_layer=apollo_lateral_semantics`.
- Fresh online validation
  `runs/phase1_online_pairs/phase1_lane_keep097_infcycle_verify_20260701_022242`
  reproduces this blocker through the live `phase1 run-pair` entry with an
  owned CARLA session. PlanningControlBackend is `status=success` with root
  `timeseries.csv` and `artifact_completeness=pass`. Apollo is evaluable but
  failed: `status=failed`, `failure_reason=route_health_failed`,
  `primary_behavior_blocker=route_lateral_error_opposes_simple_lat_lateral_error`,
  and `behavior_blocker_layer=apollo_lateral_semantics`. The Apollo row is no
  longer a wrapper timeout: the runtime completed through the nested finalized
  route-health marker, with `routing_success_count=1`,
  `planning_message_count=277`, `runtime_control_tx_count=1857`,
  `nested_exit_reason=LANE_INVASION`, `nested_route_completion_ratio≈0.011`,
  and `route_s_delta_m≈2.66`. `apollo_link_health.primary_blocker` agrees with
  the Phase 1 status:
  `apollo_lateral_semantics:route_lateral_error_opposes_simple_lat_lateral_error`.
  Rebuilding `comparison_summary.json` after the summary-surface fix now puts
  `status`, `phase1_status`, and `run_dir` directly in each `backend_results`
  row, so the comparison can be audited without cross-opening
  `participating_runs`. This is stronger Phase 1 evidence surfacing; it is not
  Apollo lane-keep success and does not close the full five-P0 matrix rerun
  requirement.
- A fresh online attempt for this blocker-surfacing path,
  `runs/phase1_online_pairs/phase1_baguang_blocker_surface_20260626_001715`,
  did not reach backend behavior because CARLA startup for
  `straight_road_for_baguang` never exposed an RPC listener. The pair writes
  `carla_session.status=not_ready`, `stop.status=stopped_after_startup_failure`,
  and `comparison_status=invalid/all_runs_invalid`. Residual-process inspection
  found no real CARLA / Apollo / bridge process after cleanup. Treat this as
  online environment evidence only; it does not contradict the previous fresh
  matrix and does not validate new behavior.

Executor update, 2026-06-25:

- `ScenarioRunExecutor` now materializes backend `preflight.json` into each
  run, includes preflight in `platform_execution_result.json`, and echoes it in
  `summary.json`.
- `ScenarioRunExecutor` now writes a final platform execution snapshot before
  refreshing fallback `phase1_status.json`, then rewrites the result with the
  status artifact paths. This lets timeout classifiers consume final dispatch
  status, effective timeout, and nested runtime artifacts in the same executor
  call instead of requiring manual reclassification.
- `BackendRuntimeAdapter` no longer silently executes only the first command in
  a multi-command launch plan. Multi-command plans are refused with
  `unsupported_multi_command` until a real pipeline lifecycle is implemented.
- `BackendRuntimeAdapter` now records cleanup status separately from runtime
  command status and postprocess status. This makes stream/process cleanup
  auditable without changing backend behavior.
- Runtime command failure no longer skips postprocess automatically. If a
  runtime command was attempted and postprocess commands are declared, the
  adapter runs safe postprocess so `phase1_status`, failure reason, and
  artifact-completeness checks can still be produced.
- Compatibility backends that write a single nested legacy `timeseries.csv` or
  `timeseries.jsonl` now get a run-local
  `analysis/phase1_artifact_normalization/phase1_artifact_normalization_report.json`.
  The executor promotes that unique nested `timeseries.*` to the declared run
  root before refreshing `phase1_status`. This is artifact-surface
  normalization only: it does not rewrite runtime behavior, does not change a
  timeout into success, and does not satisfy missing artifact-completeness
  gates by itself.
- `phase1 run-pair` no longer sets platform-level `legacy_dispatch=True` for
  real runs. Compatibility remains expressed by the selected backend launch
  plan/adapter, not by the pair runner leaking legacy implementation details.
- `phase1 run-p0-matrix` now loops the five P0 scenario pairs through the same
  `run-pair` path and writes `phase1_p0_matrix_manifest.json` plus
  `phase1_p0_matrix.csv`. This proves one-command matrix materialization and
  artifact routing; it does not prove online Apollo behavior or no-assist
  execution.
- Apollo Town01 route-only launch previews now pass the configured
  `configs/io/examples/town01_apollo_route_health.yaml` explicitly through
  `tools/run_town01_capability_online_chain.py --config ...` instead of relying
  on that tool's implicit default. This is provenance hardening only; it does
  not change Apollo runtime behavior or make the current timeout samples pass.
- Follow-up local online probe
  `runs/phase1_online_pairs/follow_stop_static_spawn2m_goal_carla_ready_20260625_113744`
  shows the next environment boundary. The pair runner now gets past the
  earlier setup blockers where `carla_builtin` used a Python without the CARLA
  API and the Apollo transition backend rejected platform preflight artifacts
  as stale runtime evidence. It still does not reach behavior evaluation:
  the package CARLA server opened port `2000` briefly, then exited before the
  fixed-scene runner could `load_world(straight_road_for_baguang)`, and Apollo
  reported no running CARLA. The comparison remains `invalid/all_runs_invalid`.
  Treat this as online environment/startup evidence, not backend behavior
  evidence.
- Keeping CARLA alive through the existing `CarlaLauncher` path then produced
  a stronger online pair:
  `runs/phase1_online_pairs/follow_stop_static_spawn2m_goal_keepalive_20260625_114446`.
  Both backend commands completed and the comparison is
  `comparison_status=comparable` with
  `comparison_target_status=apollo_vs_planning_control_evaluable`. The
  PlanningControlBackend run is `phase1_status=success`; the ApolloBackend run
  is an evaluable `lane_invasion` failure with `v_t_gap.status=warn`.
  `apollo_link_health.primary_blocker` is
  `localization_gt_contract:apollo_hdmap_projection_lateral_error_high`, with
  secondary blockers in HDMap projection, planning reference-line compatibility,
  active blocking assists, and natural-driving insufficiency. This is valid
  Phase 1 comparison evidence, not Apollo follow-stop success and not
  no-interference natural-driving evidence.
- The pair and P0 matrix runners now expose the same lifecycle as an explicit
  operator option: `--start-carla`. It starts CARLA through `CarlaLauncher`,
  records `carla_session/phase1_carla_session.json`, keeps the server alive
  for the pair or full matrix, and records stop status. In dry-run mode the
  same option records `status=dry_run_not_started` without starting CARLA. This
  removes the previous manual keepalive step from the reproducible workflow,
  but remains startup/environment evidence only; it does not prove backend
  behavior, Apollo no-assist operation, or natural driving.
- Follow-up online validation with the new `--start-carla` path,
  `runs/phase1_online_pairs/follow_stop_static_spawn2m_goal_start_carla_retry_20260625_120545`,
  did not reach backend behavior. CARLA opened RPC ports for
  `straight_road_for_baguang`, but `rpc_handshake_ready=false` after the
  180s startup window. The runner wrote
  `carla_session/phase1_carla_session.json` with `status=not_ready` and
  `stop.status=stopped_after_startup_failure`, and no CARLA process remained.
  This validates failure cleanup and artifact materialization, while leaving
  CARLA startup/RPC readiness as the next online environment blocker for this
  path.
- A shorter retest after the graceful-startup fix,
  `runs/phase1_online_pairs/follow_stop_static_spawn2m_goal_start_carla_graceful_20260625_121727`,
  confirms the pair runner now writes both backend run directories,
  `platform_execution_result.status=blocked_by_carla_startup`,
  `analysis/phase1_status.status=invalid`, and
  `comparison_status=invalid` / `reason=all_runs_invalid` instead of emitting a
  Python traceback. This is an improvement in runtime delivery diagnostics, not
  a backend behavior result.
- The next readiness fix moved `CarlaLauncher` RPC probes out of the caller's
  Python when needed. The default project interpreter is Python 3.13, while the
  local CARLA 0.9.16 package provides no cp313 wheel. The launcher now records
  this in `carla_session.diagnostics.client_probe` and uses the local
  `carla16` Python for CARLA client probes. Startup probes
  `runs/phase1_online_pairs/carla_startup_probe_town01_external_world_retry_20260625_123649`
  and
  `runs/phase1_online_pairs/carla_startup_probe_baguang_external_world_20260625_123720`
  both reach `world_ready` through the external probe. The observed initial
  world is still `Carla/Maps/Town10HD_Opt`, so startup readiness remains
  distinct from map-load identity; backend runs must still verify
  `load_world(<scenario map>)` in their own artifacts.
- With the external CARLA client probe, the unified pair entry now reaches an
  evaluable online pair without a manual keepalive:
  `runs/phase1_online_pairs/follow_stop_static_spawn2m_goal_external_probe_20260625_123157`.
  The CARLA session is `ready` and stops cleanly. The PlanningControlBackend
  run is `phase1_status=success` with `v_t_gap=pass`; the ApolloBackend run is
  evaluable and fails with `failure_reason=lane_invasion`, `v_t_gap=warn`, and
  no blocking assists in the ScenarioComparison summary. The pair comparison is
  `comparison_status=comparable` and
  `comparison_target_status=apollo_vs_planning_control_evaluable`. This is
  Phase 1 scenario-comparison evidence only; it is not Apollo behavior success
  and not no-interference natural-driving evidence.
- The first full five-P0 online matrix through the same entry is
  `runs/phase1_p0_matrix/phase1_p0_online_20260625_124044`. It materialized
  all five pairs with one matrix command and a shared `--start-carla` lifecycle:
  three rows are `completed` and comparable (`follow_stop_static`,
  `lead_decel_accel`, `cut_in_simple`), while two rows are
  `partial_or_failed` (`lane_keep_straight`, `lane_keep_curve`). The three
  Baguang pairs show PlanningControlBackend `success` with `v_t_gap=pass` and
  ApolloBackend evaluable `failed/lane_invasion` with `v_t_gap=warn`. The
  Town01 straight row is partially evaluable: PlanningControlBackend is
  evaluable but fails `lane_invasion`, while Apollo times out and is invalid
  due to missing timeseries. The curve row is `all_runs_invalid`; both
  backends lack timeseries for that row. This is the first matrix-level online
  delivery evidence, but Phase 1 Core remains incomplete until all five P0 rows
  are evaluable comparisons.
- `phase1_p0_matrix_manifest.json` now records pair-level
  `comparison_status`, `comparison_target_status`, backend Phase 1 statuses,
  backend failure reasons, and comparable/partial/invalid counts in its
  summary. This makes matrix review possible from the top-level artifact
  instead of requiring operators to open every pair directory.
- Follow-up artifact inspection of the two incomplete Town01 rows identified
  two delivery defects, not new acceptance defects. First, the legacy Town01
  Apollo route-only compatibility command was launched with the project
  default `python3`, which cannot import the local CARLA 0.9.16 Python module;
  new launch plans now choose `CARLA_TESTBED_CARLA_PYTHON`, `CARLA16_PYTHON`,
  or the local `carla16` interpreter and record that interpreter in
  `launch_plan.env` and warnings. Second, safe postprocess could complete
  without creating `analysis/phase1_status/phase1_status.json`; the executor
  now materializes a classifier fallback for attempted real runtime commands
  when postprocess misses that artifact. These are delivery-surface fixes and
  require a new online matrix before the two Town01 rows can be upgraded.
- Online validation sample
  `runs/phase1_online_pairs/lane_keep_097_route_python_fix_20260625_131051`
  confirms the Town01 Apollo route-only launch no longer fails with
  `ModuleNotFoundError: carla`: the command starts with the local `carla16`
  Python and the launch env records the same interpreter. CARLA session startup
  reached `status=ready` and stopped cleanly. The Apollo run still timed out
  before writing `timeseries.*`, so it remains `phase1_status=invalid` /
  `failure_reason=no_timeseries`; the pair is `partially_evaluable`, not a
  completed P0 row. The same sample exposed a cleanup gap where legacy child
  processes outlived the adapter timeout. `BackendRuntimeAdapter` now starts
  runtime commands in a separate process group and terminates the group on
  timeout/cleanup; this is covered by offline tests and needs the next online
  matrix to confirm under the full legacy stack.
- Follow-up full matrix
  `runs/phase1_p0_matrix/phase1_p0_online_postfix_20260625_132512` confirms
  the Python/import and missing-status fixes under the batch entry. All five
  pair manifests materialize. The three Baguang rows remain comparable
  (`follow_stop_static`, `lead_decel_accel`, `cut_in_simple`) with
  PlanningControlBackend `success` and ApolloBackend evaluable
  `failed/lane_invasion`. `lane_keep_straight` is now explicitly
  `partially_evaluable`: PlanningControlBackend is evaluable but fails
  `lane_invasion`, while Apollo times out and is `invalid/no_timeseries`.
  `lane_keep_curve` remains `invalid`: the PlanningControlBackend row failed
  while loading the CARLA world after the prior Apollo timeout, and Apollo also
  timed out with `invalid/no_timeseries`. No residual CARLA/Apollo child
  processes remained after this full matrix, so process-group cleanup improved
  the batch-level lifecycle, but the Town01 rows still do not satisfy Phase 1
  P0 completion.
- The curve failure exposed a second CARLA lifecycle gap: a single
  `client.load_world(Town01)` can fail while the server is busy or recovering
  after a legacy Apollo timeout. The builtin ego runner now uses the existing
  CARLA `load_world_with_retry()` helper for fixed-scene and route-only runs
  and writes `artifacts/carla_load_world_attempts.jsonl`. Single-pair online
  validation at
  `runs/phase1_online_pairs/curve217_world_load_retry_20260625_134436` shows
  the PlanningControlBackend curve run now reaches timeseries and becomes
  evaluable (`failed/collision`) instead of `invalid/no_timeseries`. The Apollo
  half still times out as `invalid/no_timeseries`, and that same run exposed a
  detached `examples.run_followstop` residual from the `conda run` legacy
  chain. `BackendRuntimeAdapter` now also scans for residual processes whose
  command line references the current run directory and terminates them during
  cleanup. Online validation
  `runs/phase1_online_pairs/curve217_residual_cleanup_20260625_135500`
  confirms the Apollo timeout cleanup records
  `terminated_residual_processes_for_run_dir:1`, writes
  `execution/residual_process_cleanup.json`, and leaves no matching CARLA /
  Apollo / bridge child process running. This still does not make the Apollo
  curve row evaluable; it remains `invalid/no_timeseries`.
- Because the shared matrix-level CARLA session can be polluted by a legacy
  Apollo timeout or nested `conda run` process, `phase1 run-p0-matrix
  --start-carla` now records a top-level `status=per_pair_isolated` session
  policy and delegates CARLA startup/stop to each pair. This is a lifecycle
  containment fix: one bad pair should not turn a later PlanningControlBackend
  row into `invalid/no_timeseries`. It still needs a new online matrix before
  the P0 delivery estimate can be upgraded.
- The follow-up full matrix
  `runs/phase1_p0_matrix/phase1_p0_online_pair_isolated_20260625_143833`
  validates that containment. The command exited `0`, wrote all five pair
  manifests, and left no matching CARLA / Apollo / bridge residual processes.
  Top-level `carla_session.status=per_pair_isolated`. Summary counts:
  `comparable_pair_count=3`, `partially_evaluable_pair_count=2`,
  `invalid_pair_count=0`. The three Baguang rows remain comparable with
  PlanningControlBackend `success` and ApolloBackend evaluable
  `failed/lane_invasion`. Both Town01 rows are now partially evaluable rather
  than all-invalid: PlanningControlBackend is evaluable
  (`lane_keep_straight failed/lane_invasion`,
  `lane_keep_curve failed/collision`), while ApolloBackend remains
  `invalid/no_timeseries` after timeout. This is a real Phase 1 delivery
  improvement, but still not Phase 1 completion and not Apollo behavior
  success.
- Post-hoc artifact normalization of that same matrix found a unique nested
  legacy `timeseries.csv` for each Town01 Apollo route-only run and promoted it
  to the declared Phase 1 run root. Reclassification changes both Apollo
  Town01 rows from `invalid/no_timeseries` to evaluable `failed/timeout`.
  Promoting the matching nested route-only `events.jsonl` and
  `artifacts/control_apply_trace.jsonl`, writing
  `analysis/v_t_gap/v_t_gap_report.json` with `status=not_applicable`, and
  writing Phase 1 artifact-completeness reports makes the recomputed matrix
  `5 comparable` with
  `comparison_target_status=apollo_vs_planning_control_evaluable` for all five
  rows. The post-hoc summary is
  `runs/phase1_p0_matrix/phase1_p0_online_pair_isolated_20260625_143833/analysis/phase1_posthoc_artifact_normalization/summary.json`.
  This narrows the blocker from "no frame evidence" to "fresh automatic online
  matrix validation"; it does not make Apollo successful. In the recomputed
  evidence, Apollo fails with `lane_invasion` on the three Baguang rows and
  `timeout` on the two Town01 rows, while `blocking_assists=[]` in the
  ScenarioComparison backend results.
- Fresh online single-pair validation
  `runs/phase1_online_pairs/phase1_lane_keep_auto_artifacts_20260625_152811`
  confirms the automatic path for a Town01 route-only row. The pair is
  `comparison_status=comparable` and
  `comparison_target_status=apollo_vs_planning_control_evaluable`. The
  PlanningControlBackend row is evaluable `failed/lane_invasion`; the
  ApolloBackend row is evaluable `failed/timeout`. The Apollo run root now
  automatically contains root-level `timeseries.csv`, `events.jsonl`,
  `artifacts/control_apply_trace.jsonl`,
  `analysis/v_t_gap/v_t_gap_report.json`,
  `analysis/phase1_status/artifact_completeness.json`, and
  `analysis/artifact_completeness/artifact_completeness_report.json`.
  `blocking_assists=[]`. This proves the route-only artifact-surface fix on a
  new online run, but it is still not a full fresh five-P0 matrix rerun and
  not Apollo behavior success.
- Historical full-matrix validation
  `runs/phase1_p0_matrix/phase1_p0_auto_artifacts_20260625_153515` confirms the
  older automatic artifact-surface path across all five P0 rows. That matrix
  command materialized five comparable pairs with
  `comparison_target_status=apollo_vs_planning_control_evaluable` for every
  row and left no matching CARLA / Apollo / bridge process running after
  completion. It is historical evidence and has been superseded for current
  Phase 1 status by the behavior-default matrix noted above. It does not prove
  Apollo behavior: Apollo remains `failed/lane_invasion` on
  the three Baguang rows and `failed/timeout` on the two Town01 route-only rows.
- This is runtime lifecycle and artifact-surface hardening only. It does not
  prove Apollo success or no-interference natural driving.

Delivery-first update, 2026-06-23:

- The acceptance/schema/review-pack layer is now treated as mostly frozen for
  Phase 1 unless it blocks a real valid run. New work should prioritize
  runnable delivery surfaces and behavior evidence, not more verifier layers.
- The platform now has a package-level `BackendRuntimeAdapter` and
  `ScenarioRunExecutor` path. `python3 -m carla_testbed run --plan <plan>
  --run-dir <run>` executes the resolved backend launch plan through a
  lifecycle wrapper that records PID, stdout/stderr, timeout, final platform
  execution status, and postprocess outcome. This is a delivery-surface
  improvement, not evidence that Apollo behavior succeeds.
- Phase 1 also has a one-command pair runner:
  `python3 -m carla_testbed phase1 run-pair --scenario <case> --out <dir>`.
  It compiles one PlanningControlBackend plan and one ApolloBackend plan,
  executes them sequentially, and writes a ScenarioComparison. Invalid runs
  remain invalid or partially evaluable and still do not count as backend
  losses.
- The remaining Phase 1 completion gap is runtime delivery and behavior:
  Apollo must own ego without blocking legacy assists on representative P0
  scenarios, the target interaction must actually happen, and the paired
  backend results must enter comparison from one command. The new executor
  makes this path explicit but does not by itself remove those blockers.
- Latest Baguang `follow_stop_static` online pair,
  `runs/phase1_online_pairs/follow_stop_static_20260623_215028`, reduces one
  Apollo handoff ambiguity. The Phase 1 Apollo compat config now requires
  routing success before `planning_ready` may start control, and the gate
  consumes bridge-health aliases such as `routing_first_success_response_ts_sec`.
  In that run `apollo_control_planning_ready_wait.log` reached `status=ready`
  with `route_established=True`, then Apollo control mainboard crashed with
  `tcmalloc.cc:333] Attempt to free invalid pointer`. The run remains an
  evaluable Apollo failure (`phase1_status.failure_reason=control_process_failed`)
  and the comparison remains `partially_evaluable`; this is blocker reduction,
  not Apollo behavior success.
- The next launcher-side reduction is now encoded in the Phase 1 ApolloBackend
  facade: static Baguang follow-stop LaunchPlans default to the diagnostic full
  control-runtime overlay profiles
  (`phase1_baguang_apollo_followstop_static_control_overlay_paced_compat.yaml`
  and the spawn2m low-capture paced equivalent). This follows the 6/18 online
  bisection evidence that no-overlay runs expose `tcmalloc_invalid_free`, while
  full-overlay runs can move past control process survival into behavior-level
  failures. The no-overlay profiles remain available as reproduction controls;
  the overlay is not a natural-driving or unassisted Apollo claim.
- Follow-up online pair
  `runs/phase1_online_pairs/follow_stop_static_20260623_2230_overlay_survival_fix`
  confirms that boundary moved again: Apollo control starts after routing,
  survives the 5s deferred probe, and produces `/apollo/control`,
  bridge-receive, and CARLA apply evidence during the active run window. A
  later 10s deferred survival sample observed control absent only after
  `events.jsonl` had already recorded `run_end`, so the refreshed handoff
  analyzer now treats that lifetime sample as
  `control_lifetime_after_run_end_not_evaluable` instead of an in-run Apollo
  control crash. The refreshed control-health report is `warn`, not a handoff
  hard fail. The run still fails Phase 1 (`lane_invasion`; comparison
  `partially_evaluable`) and remains diagnostic only because `legacy_followstop`
  is a blocking assist. The current highest blocker for this sample is now
  explicitly reported as `lane_event_contract_quarantines_lane_invasion`: both
  Apollo and builtin runs are evaluable, but the shared near-start
  LaneInvasionSensor trigger is inconsistent with centerline/static-footprint
  evidence, so it is not counted as a backend behavior loss. The next useful
  fix is to repair or explain the Baguang lane-event/spawn/OpenDRIVE
  road-marking contract, not total bridge breakage or a proven Apollo
  control-process crash. The next follow-stop online sample should use the
  unified pair entry with the diagnostic spawn mitigation:
  `python3 -m carla_testbed phase1 run-pair --scenario
  baguang/follow_stop_static_300m_spawn2m --out <out> --timeout-s <sec>`.
  Dry-run coverage now verifies that this path compiles both backend plans and
  selects the Apollo spawn2m paced overlay profile.
- Spawn-mitigated online pair
  `runs/phase1_online_pairs/follow_stop_static_spawn2m_20260623_232345`
  confirms the next boundary. The unified pair runner executed both backends
  successfully. PlanningControlBackend finished with `phase1_status=success`,
  `lane_invasion_count=0`, and `v_t_gap.status=pass`. ApolloBackend produced
  routing, non-empty Planning, `/apollo/control`, bridge receive/apply, obstacle
  GT, derived scenario actor trace, and `v_t_gap.status=warn`, but failed with
  one lane invasion after about `14.3m` from the shifted start and
  `cross_track_error≈0.667m`.
  A postprocess-order fix now derives Apollo fixed-scene compatibility artifacts
  from `obstacle_gt_contract.jsonl` before writing `fixed_scene_contract` and
  `scenario_actor_contract`, so the same run refreshes to
  `fixed_scene_contract.status=pass`, `scenario_actor_contract.status=pass`,
  `phase1_status.scenario_interaction_evaluable=true`, and
  `counts_as_backend_loss_for_target_scenario=true`.
  The refreshed comparison is therefore `comparison_status=comparable` and
  `comparison_target_status=apollo_vs_planning_control_evaluable`: builtin
  succeeded, Apollo is an evaluable lane-invasion behavior failure. This is
  stronger Phase 1 comparison evidence, not Apollo follow-stop success and not
  no-interference natural-driving evidence. The refreshed Phase 1 status now
  also surfaces `path_candidate_control_context` directly: Control target
  points lie inside the diagnostic Planning path-candidate lateral envelope and
  near lane center, while path candidates span about `1.25m` lateral offset
  from the HDMap lane center. This narrows the next Apollo investigation to
  Planning reference-line/debug export and lateral semantics, but remains
  diagnostic-only until exported Planning reference-line evidence is
  claim-grade.
- Dynamic lead-deceleration online pair
  `runs/phase1_online_pairs/lead_decel_70_to_40_20m_20260624_001954`
  validates that the unified `phase1 run-pair` entry can execute a non-static
  Baguang fixed-scene sidecar for both PlanningControlBackend and ApolloBackend.
  PlanningControlBackend completed successfully with target actor and
  `v-t-gap` evidence. ApolloBackend completed the runtime command and produced
  routing/planning/control, obstacle GT, scenario actor trace, and `v-t-gap`,
  but the comparison is `partially_evaluable` with
  `comparison_target_status=initial_conditions_not_materialized`. The scenario
  declares `ego_initial_speed_mps=19.44`; the refreshed Phase 1 status observes
  Apollo's initial ego speed window near `0.19m/s`, so the lead vehicle moves
  away and the target interaction is not exercised before the Apollo lane
  invasion. This is a Phase 1 setup/runtime materialization blocker, not an
  Apollo lead-deceleration behavior loss. The next high-value runtime fix is
  to decide how external backends materialize nonzero initial ego speed for
  dynamic fixed scenes, or to mark such scenarios incompatible with the current
  Apollo sidecar until that capability exists.
- Follow-up dynamic lead-deceleration online pair
  `runs/phase1_online_pairs/lead_decel_70_to_40_20m_initial_speed_20260624_003219`
  clears that setup blocker. The Apollo fixed-scene sidecar now writes
  `artifacts/ego_initial_state_materialization.json` and applies the scenario
  `ego_initial_speed_mps=19.44` as a one-shot setup action before the backend
  owns ego control. The refreshed Phase 1 status observes Apollo's initial ego
  speed window at about `18.96m/s`, so target interaction is now evaluable.
  The comparison refreshes to `comparison_status=comparable` and
  `comparison_target_status=apollo_vs_planning_control_evaluable`: builtin
  succeeds, while Apollo fails with `collision` at about `16m` bumper gap. This
  is a real Phase 1 behavior blocker, not a natural-driving claim and not
  evidence that Apollo lead-deceleration works. The next high-value Apollo
  investigation has narrowed to longitudinal planning/control handoff for this
  dynamic target case: Planning produced one non-empty trajectory message, but
  the first observed control consume happened about `2.27s` later, when the
  non-empty Planning evidence was stale and control still reported zero input
  trajectory points. The refreshed handoff report now classifies this as
  `planning_nonzero_stale_before_control_consume`, which is more precise than
  the older generic "Planning non-empty but control input zero" diagnosis. The
  route-established diagnostic profile was then tested in
  `runs/phase1_online_pairs/lead_decel_70_to_40_20m_route_established_20260624_005806`.
  It correctly wrote `apollo_control_route_established_wait.log`, but still
  failed with the same stale handoff: routing success and the only non-empty
  Planning message arrived together, and Control's first consume followed about
  `2.29s` later. The next minimal runtime hypothesis is therefore encoded in
  `configs/io/examples/phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml`:
  keep the diagnostic overlay/low-capture/pacing stack, but start Control with
  the Apollo module set so it can subscribe before the short dynamic-scene
  Planning window. Fresh online pair
  `runs/phase1_online_pairs/lead_decel_70_to_40_20m_eager_control_20260624_010827`
  validates that this moves the blocker again. Apollo Control is now warm
  before the first non-empty Planning window, the refreshed handoff report is
  `warn` with `failure_stage=none`, and the run contains `101` non-empty
  Planning messages plus `725` non-zero Control input trajectory rows. The
  Phase 1 classifier also now treats the leading v-t-gap speed window as the
  initial-state evidence, so the one-shot setup artifact's first-tick speed lag
  no longer makes the run `initial_conditions_not_materialized`. The refreshed
  comparison is `comparison_status=comparable` and
  `comparison_target_status=apollo_vs_planning_control_evaluable`: builtin
  succeeds, while Apollo is an evaluable `lane_invasion` failure. This is a
  stronger dynamic-case behavior blocker, not Apollo lead-deceleration success
  and not a natural-driving claim. The raw Apollo link-health primary blocker
  may still surface no-assist/natural-driving claim-boundary layers; Phase 1
  status therefore also exposes `phase1_relevant_apollo_link_blocker`, which
  filters those claim-only blockers and points this sample back to
  `apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate`.
  A new diagnostic-only profile,
  `configs/io/examples/phase1_baguang_apollo_dynamic_sidecar_eager_control_steer_sign_inverted_diagnostic.yaml`,
  keeps the same eager-control / low-capture / pacing stack but sets
  `control_mapping.steer_sign=-1.0` for the next online A/B. This is a
  steering-sign semantics probe only: it is not a default profile, not a
  steer-scale or physical-mapping change, and not behavior success evidence
  without fresh paired artifacts. The first online single-run sample,
  `runs/phase1_online_pairs/lead_decel_70_to_40_20m_steer_sign_inverted_20260624_014350`,
  completed with
  `summary.success=true`, `collision_count=0`, `lane_invasion_count=0`,
  `phase1_status.status=success`, and `v_t_gap.status=pass`. This is strong
  diagnostic evidence that the previous lane-invasion blocker is sensitive to
  steering sign/control semantics, but it is not yet a paired backend
  comparison and not a default-profile promotion. A follow-up online rerun
  after adding explicit `steer_scale` trace fields was blocked by local CARLA
  startup failure (`未检测到运行中的 CARLA`), so the new trace-field materialization
  is currently unit-tested but not yet online-verified.
  The next highest-value Apollo investigation is lateral semantics /
  reference-line / lane-event context for the dynamic Baguang case, not stale
  control handoff.

- 2026-07-01 Town01 lane-follow root-cause status: no-guard `lane_keep_097`
  and `curve217` online samples now agree on the first confirmed control-chain
  defect.  Apollo routing/planning/control are materialized, raw-to-mapped and
  mapped-to-applied steering are internally consistent, and vehicle yaw-rate
  responds to the applied CARLA steer.  With the old `steer_sign=+1.0`, however,
  applied steering has the same sign as CARLA route cross-track error and the
  lateral error grows; the diagnostic `steer_sign=-1.0` probe substantially
  reduced `lane_keep_097` cross-track p95.  The active Phase 1 Apollo Town01
  config is therefore switched to
  `configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml`,
  where `control_mapping.steer_sign=-1.0`; steer scale, PID, physical mapping,
  and lateral guards are unchanged.  This is a control-semantics root-cause
  candidate fix, not lane-keeping completion: the same evidence still
  shows Planning reference-line/path-bound and target-point anomalies, so fresh
  no-guard straight, guarded straight, and curve runs must pass the usual
  route-health, reference-line, lateral-semantics, control-attribution,
  link-health, and Phase 1 status gates before any behavior claim.
  Fresh no-guard validation after the config change confirms the first-layer
  improvement but not behavior success:
  `phase1_lane_keep097_control_sign_fix_no_guard_20260701_125023` reduces
  `lane_keep_097` cross-track-error p95 to about `0.84m`, and
  `phase1_curve217_control_sign_fix_no_guard_20260701_125835` reduces
  `curve217` cross-track-error p95 to about `0.94m`.  Both still fail
  route-health / Phase 1 status, and both continue to report
  `control_attribution.dominant_breakpoint=source_control_semantics` with
  Apollo raw steer p95 at saturation plus reference-line or target-point
  semantics warnings.  The next repair target is therefore Apollo target-point /
  reference-line / path-bound semantics, not bridge apply, steer scale, PID, or
  guard policy.

Current local accepted comparison-surface catalog status using
`tools/phase1_scenario_catalog.py --repo . --evidence-root runs`:

- Total scenarios tracked: `8`
- Accepted `DONE`: `0`
- `PARTIAL`: `8`
- `NOT_YET`: `0`
- `UNKNOWN`: `0`

Pro-audit reconciliation:

- Earlier local `8 DONE / 0 PARTIAL` readings are now intentionally invalidated
  by the verifier hardening. Existing acceptance bundles in `runs/` were
  generated before `acceptance_verification.verification_status=passed` became
  mandatory, so the refreshed catalog correctly reports `0 DONE / 8 PARTIAL`.
- This regression in the DONE count is a trust-chain improvement. It means
  `comparison_summary.json`, `phase1_acceptance_report.json`, and review-pack
  manifests can no longer self-certify completion without being checked against
  actual run artifacts.
- It is not automatically transferable to an external review package. If the
  submitted package omits generated catalog JSON/MD, comparison directories,
  exact participating run directories, raw `timeseries.*`, actor/phase/control
  traces, `phase1_status.json`, artifact-completeness reports, assist ledgers,
  verifier output, and a SHA256 manifest, then the package-level audit status
  is `PARTIAL`.
- The previous local `Phase1ReviewPack` archive should be treated as stale for
  completion claims because it predates the independent acceptance and
  review-pack verification checks. A new review pack must be rebuilt only after
  regenerating verifier-passed acceptance bundles.
- The latest Pro audit explicitly rejected the stronger "Phase 1 complete" /
  "8 scenarios externally DONE" reading. Treat all current rows as `PARTIAL`
  until the exact accepted bundles are regenerated under the verifier and can
  be independently recomputed from packaged raw artifacts and hashes.
- Phase 1 progress must therefore be reported in three layers:
  local accepted comparison-surface catalog = `PARTIAL`;
  package-level review surface = `PARTIAL until rebuilt with verifier-passed
  bundles`; Phase 1 overall = `PARTIAL` until representative Apollo online
  behavior blockers and the external-backend runtime path are reduced.

Interpretation:

- The Phase 1 platform skeleton is substantially built: scenario definitions,
  target actor contracts, `v-t-gap`, status classification, ScenarioComparison,
  acceptance verification, review-pack materialization checks, and Apollo
  sidecar evidence all exist.
- The latest Pro audit conclusion should be read literally: Phase 1 is not
  complete yet. The accepted-bundle surface is a useful mechanism and current
  catalog milestone, but it is not the same thing as a Phase 1 completion
  certificate. Phase 1 completion still requires representative same-case
  online bundles that bind exact run IDs, complete artifacts, target
  interaction validity, metric validity, assist status, and comparison verdicts
  without mixing best evidence across unrelated runs.
- The latest Pro-review progress reading is: Phase 1 has moved from artifact
  scaffolding to evidence-backed blocker reduction. The accepted
  comparison-surface catalog is now conservatively blocked until acceptance
  bundles are regenerated through the verifier, and Phase 1 is not complete
  until representative online runs demonstrate stable external-backend behavior
  on the same scenario cases. In practical terms, the next cycle should rebuild
  verifier-passed bundles from actual run artifacts, then optimize the current
  highest-value Apollo behavior blocker.
- The newest online validation sample,
  `runs/phase1_apollo_sidecar_cut_in_planning_debug_presence_online_20260621_194107`,
  keeps that same boundary: it is an evaluable ApolloBackend behavior failure
  (`LANE_INVASION_HARD`), not a Phase 1 success. Its value is narrower but
  important: the updated bridge proves `debug.planning_data` and
  `debug.planning_data.reference_line` are present in Apollo Planning messages,
  while the reference-line list is empty
  (`planning_debug_presence.last_diagnosis=routing_present_reference_line_empty`,
  `reference_line_nonempty_ratio=0.0`,
  `routing_segment_nonempty_ratio=0.5167`). This reduces the old reference-line
  debug/export uncertainty from "maybe parsed the wrong path" to "Planning
  debug/content reports routing segments but no reference-line entries for this
  run." It does not change Planning, Control, actuation, or the Phase 1 status.
  The Phase 1 status report now carries that exact classification into
  `reference_line_debug_export_policy` and `behavior_next_action`, so operators
  should not re-debug whether the bridge found the Planning debug path. The
  next question is why Apollo Planning has routing/debug content but an empty
  `debug.planning_data.reference_line` list for this route.
  The refreshed reference-line contract now adds
  `planning_materialization_summary`: in the claim window for this run,
  `create_route_segments_status=ready`, `planning_stage_name=LANE_FOLLOW_STAGE`,
  and `task_name=LANE_FOLLOW` for all route-ready rows, while the exported
  reference-line list remains empty and trajectories are non-empty. This
  narrows the blocker from "maybe Planning did not enter lane follow" to
  "lane-follow route segments and trajectories are present, but exported
  reference-line debug is empty." It is still attribution evidence only and
  does not fix lane invasion or prove behavior success.
  The reference-line export policy now records this as
  `reference_line_debug_field_state=field_present_but_empty_after_lane_follow_materialization`.
  That wording is intentional: the Planning debug field is present, so
  operators should not re-debug the parser path as missing; however, the list
  is still empty after lane-follow materialization, so the evidence remains
  non-claim-grade and the next action is to inspect Planning
  ReferenceLineInfo/debug population for this route before changing steering,
  smoothing, PID, or actuation mapping.
  The next analyzer refresh consumes `artifacts/apollo_planning.INFO` as
  text-log diagnostic evidence. In the same run, the log contains internal
  reference-line traces (`print_regular/self_ref_l`, `print_st_reference_line`,
  `print_vt_reference_line`, and `print_trajxy`) plus reference-line ST-boundary
  construction records, while `debug.planning_data.reference_line` in the
  Planning message remains an empty list. This further narrows the question to
  ADCTrajectory debug/reference-line population or export semantics. The text
  log is explicitly `claim_grade_allowed=false`; it does not prove
  reference-line correctness or behavior success.
  The next bridge/analyzer cycle also records a compact Planning debug field
  inventory and candidate reference/path-like repeated fields. This is intended
  to distinguish "Apollo 10 really exports an empty
  `debug.planning_data.reference_line` list" from "the bridge is still looking
  at a narrower debug field than Apollo actually populated." The inventory is
  diagnostic-only and remains below claim-grade evidence until it is joined with
  exported reference-line semantics or an explicit equivalent contract.
  A fresh online validation,
  `runs/phase1_apollo_sidecar_cut_in_field_inventory_online_20260621_215906`,
  keeps the same behavior boundary (`LANE_INVASION_HARD`) and confirms the
  inventory is materialized in real artifacts. In that run,
  `debug.planning_data.reference_line` remains present-but-empty, while
  `debug.planning_data.path` is a non-empty candidate
  (`planning_debug_path_candidate_evidence.classification=path_candidate_present_reference_line_empty`,
  max repeated count `4`). The analyzer exposes this as diagnostic path
  evidence only (`reference_line_claim_grade_allowed=false`); it narrows the
  next parser/export question but does not replace claim-grade reference-line
  evidence or change the Phase 1 status.
  The next bridge/parser increment now records
  `planning_debug_path_candidate_summary` for those path-like debug fields:
  candidate path, item count, shallow scalar fields, nested sequence counts,
  and point-like sequence samples when present. This is explicitly
  diagnostic-only. Its purpose is to decide whether `debug.planning_data.path`
  can become an explicit equivalent diagnostic path contract in a later cycle;
  it still cannot make `debug.planning_data.reference_line` pass or turn a
  lane-invasion run into backend success.
  Online validation in
  `runs/phase1_apollo_sidecar_cut_in_path_candidate_online_20260621_221537`
  confirmed this field is not just a fixture artifact: the Planning debug
  summary reports `debug.planning_data.path` with four entries, including
  `planning_path_boundary_1_regular/self` and
  `planning_path_boundary_2_regular/self`, each with hundreds of `path_point`
  samples. The refreshed reference-line report classifies this as
  `path_candidate_points_present_reference_line_empty` and still marks
  `reference_line_claim_grade_allowed=false`. The run itself remains a
  `LANE_INVASION_HARD` ApolloBackend behavior failure.
  The next diagnostic increment samples those path candidate point sequences
  and compares them with same-row Planning trajectory samples. A fresh online
  run,
  `runs/phase1_apollo_sidecar_cut_in_path_samples_online_20260621_223658`,
  verifies that `sample_points` are now materialized in
  `planning_debug_path_candidate_summary`. Its refreshed report exposes
  `planning_debug_path_candidate_vs_trajectory_sample.classification=planning_debug_path_candidate_offset_from_planning_trajectory_sample_support`,
  `sample_coverage_ratio=1.0`, and
  `path_candidate_to_planning_sample_line_abs_p95_m≈3.08m` inside the
  trajectory sample support window; the full-horizon path-to-trajectory p95 is
  also reported separately because path candidates may extend far beyond the
  short trajectory sample slice. This is a useful bridge/export attribution
  check: it shows `debug.planning_data.path` is not just a count-only field,
  but it also reinforces that `path` entries can be boundary or optimizer debug
  paths rather than the exported Planning reference-line centerline. It remains
  `reference_line_claim_grade_allowed=false`, and the new online run remains a
  `LANE_INVASION` ApolloBackend behavior failure.
  The same report now joins sampled `debug.planning_data.path` points to
  official Apollo HDMap projection rows. In the latest sample,
  `planning_debug_path_candidate_hdmap_projection_alignment.classification=planning_debug_path_candidate_lateral_offset_from_hdmap_lane_center`,
  `routing_lane_window_compatible=true`,
  `nearest_lane_id_topk=["0_0_2"]`, and
  `path_candidate_lane_l_abs_p95_m≈1.255m` with near-zero heading error. This
  narrows the debug/export interpretation: the path candidates are on the
  expected Apollo lane window, but their lane-lateral distribution is
  boundary/optimizer-like rather than centerline-like. It is still
  diagnostic-only and cannot be promoted to claim-grade reference-line evidence.
  The next refresh joins Control `simple_lat` target points against the sampled
  path candidates and the same official HDMap projection rows. For the same
  sample,
  `control_target_point_vs_planning_path_candidate_sample.classification=control_target_between_planning_path_candidate_lateral_bounds`:
  the Control target lane-l p95 is near zero
  (`≈4.9e-7m`), while the path-candidate lateral envelope spans roughly
  `[-1.255m, +0.82m]` and the target-to-candidate-line p95 is about `0.82m`.
  This reinforces the diagnostic interpretation that `debug.planning_data.path`
  entries are likely boundary/optimizer candidate paths around the local target
  trajectory, not exported reference-line centerlines. It still does not change
  behavior status or allow reference-line claim-grade evidence.
  The next link-health refresh joins the same lane-invasion event with the
  lane-event contract, failure timeline, yaw-aware footprint check,
  raw/mapped/applied steering, and Control-target/path-candidate context. For
  `runs/phase1_apollo_sidecar_cut_in_path_samples_online_20260621_223658`, the
  merged context is
  `lane_invasion_context.classification=progressive_lane_departure_with_control_target_between_path_candidate_bounds`.
  The event occurs about `22.55m` after start, footprint intersection with the
  crossed marking is true, trigger clearance is about `-0.115m`, CTE grows by
  about `0.474m`, mapped-to-applied steer error is `0.0`, and raw-to-mapped
  steer gain is about `0.247`. The Control target remains inside the sampled
  path-candidate lateral envelope while the reference-line contract still has
  `reference_line_claim_grade_allowed=false`. This closes a diagnostic
  evidence gap; it does not fix the lane invasion, does not make the run pass,
  and does not make `debug.planning_data.path` a claim-grade reference line.
  The latest control-health refresh adds `lane_event_response_context` for the
  same event. It classifies the 0.95s pre-event window as
  `applied_steer_yaw_response_tracks_progressive_lateral_departure`: CTE grows
  by about `0.474m`, heading error grows by about `0.111rad`, applied steer and
  yaw-rate have the same sign, and CARLA vehicle-response rows are available.
  This narrows the next step away from CARLA apply-drop or missing yaw
  response. It does not prove Apollo Control is correct; it says the applied
  command was physically materialized and the vehicle response tracked the
  progressive departure, so the next highest-value analysis is
  path-boundary/reference-line semantics versus downstream control target
  choice.
  The newest lane-event contract refresh folds both pieces into the lane-event
  hard-gate audit itself:
  `lane_event_attribution.classification=progressive_lane_departure_with_vehicle_response_and_control_target_between_path_candidate_bounds`.
  The refreshed link-health report now carries the same classification in
  `natural_driving_outcome.key_metrics.lane_invasion_context`. This is a
  cleaner attribution surface for audit packages, but it deliberately does not
  change the run status: the report remains a lane-invasion failure,
  reference-line claim-grade remains false, and no-assist/natural-driving
  claims remain blocked.
  The same attribution now also carries the route-lateral sign policy from
  `phase1_status`: `route_lateral_sign_policy=exclude_from_sign_sensitive_behavior_gates`,
  `source_field=cross_track_error`, and
  `route_lateral_sign_sensitive_gate_allowed=false`. This prevents the
  `same_sign_as_cross_track_error` diagnostics from being read as a steering
  sign or control-direction root cause. Until the route-lateral field is
  relabeled or explicitly converted against Apollo projection/simple_lat
  convention, only absolute-magnitude lane-event evidence is claim-relevant.
  The lane-event report now also surfaces this in a top-level
  `representative_run_context`, including the representative attribution,
  route-lateral sign policy, and projection/simple_lat context. This is an
  audit-readability improvement only: it does not change the underlying
  `run_reports[]`, does not make the lane-invasion run pass, and does not
  promote diagnostic path/projection evidence to claim-grade reference-line
  evidence.
  The same cycle fixed a claim-boundary regression in `apollo_link_health`:
  a derived `analysis/assist_ledger/assist_ledger.json` with `source_artifact=config`
  can no longer clear a blocking assist declared in `manifest.json` or
  `summary.json`. The refreshed link-health report for that run correctly
  reports `primary_blocker=no_assist_claim_boundary:blocking_assists_active`
  with `active_assists=["legacy_followstop"]`. This prevents diagnostic
  compatibility runs from being misread as no-interference Apollo runs.
- The latest Pro audit found that the earlier `8/8 DONE` statement was still
  too optimistic because some review-pack surfaces were stale or could not be
  independently re-computed. The follow-up fix keeps accepted-bundle rows tied
  to self-contained materialized evidence and makes ScenarioComparison use the
  Phase 1 artifact-completeness profile rather than the stricter natural-driving
  claim/materialization profile.
- The corrected reading is therefore not "Phase 1 is complete." The corrected
  reading is: "the local eight-row accepted comparison-surface catalog is a
  useful artifact surface, but Phase 1 overall and any external/package-level
  completion claim remain `PARTIAL` until the exact run pairs, raw traces,
  comparison artifacts, artifact-completeness reports, assist ledgers, and
  hashes are packaged so the result can be independently recomputed."
- The latest local package-level surface has now been generated with
  `tools/package_phase1_review_pack.py`:
  `artifacts/phase1_completion_review_pack_current.tar.gz`
  (`sha256=9b96e51d0e841acd79ff36eb8a2ff77b2374ade616dce733c65dd00c21c70ea2`).
  Its `phase1_review_pack_manifest.json` reports `status=DONE`,
  `scenario_count=8`, `done_scenario_count=8`, and zero missing required items.
  This closes the local self-contained pack construction gap, but it remains a
  package-level audit surface pending external review; it still does not prove
  Apollo behavior success or Phase 1 overall completion.
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
  Future bridge runs now record `planning_debug_presence` in
  `planning_topic_debug_summary.json`, `planning_topic_debug.jsonl`, and
  `planning_route_segment_debug.jsonl`. This distinguishes
  `debug.planning_data` missing, reference-line field absent, reference-line
  field present-but-empty, and routing-present/reference-line-empty cases. It
  closes an observability gap around the current primary blocker; it does not
  change Planning, Control, or actuation behavior.
  Online validation in
  `runs/phase1_apollo_sidecar_cut_in_planning_debug_presence_online_20260621_194107`
  produced `routing_present_reference_line_empty`: Planning debug had
  `debug.planning_data.reference_line` present, but no reference-line entries,
  while routing segments were present in roughly half the recent messages. The
  next highest-value cycle is therefore to inspect Apollo Planning
  reference-line materialization/config/debug content for this route, not to
  rediscover the debug path and not to tune control mapping.
  The postprocess now reports the materialization window explicitly:
  `route_segments_ready_ratio=1.0`,
  `reference_line_empty_with_route_segments_ready_ratio=1.0`, and
  `trajectory_nonempty_route_segments_ready_reference_line_empty_ratio=1.0`
  for that sample's route-ready claim window.
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
  The analyzer avoids being misled by a root-level `route.json` stub by
  selecting the best available route-definition evidence: materialized route
  geometry first, declared route samples second, and stubs last. The route
  contract now also materializes projection-derived route samples into
  `artifacts/route_definition_claim.json` when `apollo_hdmap_projection.jsonl`
  contains `sample_type=start|route|goal` rows. On this run the refreshed claim
  contains `61` samples with
  `scenario_route_sample_source=apollo_hdmap_projection_route_samples`,
  `frame=apollo_map`, `route_s=0..300m`, `projection_s≈7.006..307.006m`, and
  lane key `0:2`. The lateral report now records
  `route_definition_geometry_status=materialized_geometry_available` and
  `route_definition_sample_count=61`. The lateral report now uses those
  projection-derived Apollo-map samples as an explicit
  `projection_route_sample_sign_contract`: it recomputes signed lateral from
  same-row Apollo-map ego positions and nearest route samples without mixing in
  CARLA-frame coordinates. On the same run, that contract is `available` with
  `matched_sample_count=335`,
  `timeseries_lateral_vs_projection_route_sample_signed_lateral.opposite_sign_ratio=1.0`,
  and
  `simple_lat_vs_projection_route_sample_signed_lateral.same_sign_ratio=1.0`.
  This is stronger than a route-stub or alias-only sign suspicion, but it is
  still diagnostic route/projection evidence: the samples are derived from
  Apollo HDMap projection rows, not from canonical CARLA route geometry, and
  they do not convert a failed lane-invasion run into a behavior pass.
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
- After the latest Pro review, Phase 1 progress is now clearer but still not
  accepted: the Apollo cut-in sample has enough official projection/simple_lat
  evidence to classify the lateral convention gap, but it still fails behavior
  status with lane invasion. The refreshed
  `lateral_frame_convention_diagnostic` classifies the sample as
  `route_lateral_sign_inverted_vs_apollo_projection_candidate`: 13 official
  projection-matched samples show
  `route_lateral_vs_projection_lateral.opposite_sign_ratio=1.0`,
  `simple_lat_vs_projection_lateral.same_sign_ratio=1.0`, and the
  route/simple_lat lateral-error pairing remains opposite-sign with matching
  magnitude (`opposite_sign_ratio=1.0`,
  `opposite_sign_abs_sum_p95_m≈0.028m`). This is a strong diagnostic that the
  current `cross_track_error` convention is inverted relative to Apollo
  `projection_l` / Control `simple_lat` for this run. The follow-up
  projection-route sign contract now confirms the same direction using
  `route_definition_claim.scenario_route_samples` and same-row Apollo-map ego
  positions. It is not a runtime fix, not a route contract verification by
  itself, and not a reason to flip signs in control; the next validation should
  close the Planning reference-line debug/export gap and decide whether the
  route-lateral field should be relabeled, converted, or excluded from
  behavior gates until a canonical sign convention is declared.
- `apollo_link_health` now uses that lateral-semantics warning as the
  representative Apollo cut-in primary blocker when all upstream link layers
  are non-blocking and `natural_driving_report.json` is merely absent. The
  refreshed primary blocker is
  `apollo_lateral_semantics:route_simple_lat_sign_convention_mismatch_candidate`;
  `natural_driving_outcome:insufficient_data` remains a secondary blocker and
  still prevents any unassisted natural-driving claim. The reference-line debug
  export gap remains important context, but it is no longer the latest primary
  blocker for this specific cut-in sample.
- After the latest link-health refresh, `next_highest_value_validation` no
  longer asks operators to re-verify a sign convention already supported by
  projection-route samples. For the same representative cut-in run it now says
  to close the Planning reference-line debug/export gap and decide whether the
  route-lateral field should be relabeled, explicitly converted, or excluded
  from sign-sensitive behavior gates before changing control mapping,
  `steer_scale`, smoothing, or controller gains. This is a triage-quality
  next-action update only; it does not change the run's `failed/lane_invasion`
  status or make the reference-line evidence claim-grade.
- `phase1_status` now consumes the projection-route sign contract when
  explaining that same cut-in lane invasion. Its refreshed
  `behavior_next_action` no longer asks operators to re-validate the sign
  convention from scratch; it records that projection-route sample evidence
  already confirms the inversion candidate for this run and redirects the next
  action to closing the Planning reference-line debug/export gap and deciding
  whether the route-lateral field should be renamed, explicitly converted, or
  excluded from behavior gates until the canonical sign convention is declared.
  The lateral analyzer now makes that policy explicit as
  `route_lateral_field_semantics`: for the latest cut-in sample,
  `classification=route_lateral_field_opposite_signed_to_apollo_projection`,
  `sign_sensitive_gate_allowed=false`,
  `absolute_magnitude_gate_allowed=true`, and
  `recommended_gate_policy=absolute_magnitude_only_until_canonical_sign_declared`.
  This keeps the run as `failed/lane_invasion`; it only makes the next
  debugging step and gate semantics less stale.
- `phase1_status.json` now echoes the same decision at top level as
  `route_lateral_field_policy`. For the latest representative cut-in run the
  policy is `exclude_from_sign_sensitive_behavior_gates`, with
  `source_field=cross_track_error`, `sign_sensitive_gate_allowed=false`, and
  `absolute_magnitude_gate_allowed=true`. This is a Phase 1 analysis/gating
  contract only: it does not change runtime control, does not flip steer signs,
  and does not convert the Apollo lane-invasion sample into a behavior pass.
- `phase1_status.json` also now echoes
  `reference_line_debug_export_policy`. For the same run the policy is
  `local_surrogate_only_until_reference_line_debug_exported`: Planning first
  trajectory points align locally with official HDMap projection and Control
  `simple_lat` reference evidence is visible, but exported Planning
  reference-line debug counters remain unavailable. The policy keeps
  `reference_line_debug_claim_grade_allowed=false`, so these local surrogates
  narrow attribution without becoming a claim-grade reference-line pass.
- The refreshed reference-line report now includes a field inventory for this
  gap. On the same run it classifies the evidence as
  `reference_line_counter_missing_but_planning_control_surrogates_present`:
  `reference_line_count_positive_count=0`, while
  `trajectory_sample_rows=170`,
  `planning_route_segment_count_positive_count=171`,
  `routing_segment_count_positive_count=171`,
  `routing_road_count_positive_count=171`, and
  `control_target_point_rows=126`. The older broad
  `route_segment_count_positive_count` field is retained only as a compatibility
  alias for broad route/routing availability and must not be read as
  claim-grade reference-line segment evidence. This confirms the next blocker
  is not a total Planning/Control evidence absence; it is the missing exported
  Planning reference-line counter/debug evidence needed before claim-grade
  reference-line conclusions.
- The same inventory is now split by claim window. In the
  `after_routing_segment_available` window, the representative cut-in run has
  `row_count=171`, `reference_line_count_positive_count=0`,
  `trajectory_sample_rows=170`, and `control_target_point_rows=126`; provider
  status is dominated by `trajectory_nonzero_debug_missing`. This removes the
  ambiguity that startup `failed` rows alone caused the reference-line warning:
  the exported reference-line counter remains unavailable after route and
  trajectory evidence are materialized.
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
- After the latest Pro-audit progress update, this reference-line evidence is
  now split into explicit policy as well as raw diagnostic. The refreshed
  `apollo_reference_line_contract_report.json` for
  `runs/phase1_apollo_sidecar_cut_in_event_append_online_20260620_195742`
  records
  `reference_line_debug_export_policy.classification=reference_line_debug_export_gap_with_local_planning_and_control_reference_evidence`,
  `reference_line_debug_claim_grade_allowed=false`, and
  `recommended_evidence_policy=local_surrogate_only_until_reference_line_debug_exported`.
  This means Planning first trajectory points and Control `simple_lat` local
  evidence are useful attribution surrogates, but they do not prove
  claim-grade reference-line correctness until Planning reference-line debug /
  route-segment / Control station semantics are exported or decoded in one
  frame. Phase 1 remains `PARTIAL`; the current Apollo sample remains
  `failed/lane_invasion`.
- The reference-line analyzer now also preserves Planning
  `trajectory_sample_points` from `planning_topic_debug.jsonl` when an existing
  `apollo_reference_line_contract.jsonl` is present. On the same refreshed run,
  `planning_trajectory_sample_surrogate.classification=planning_trajectory_sample_surrogate_same_route_lane_window`,
  `sample_coverage_ratio=1.0`, `planning_lane_id_topk=["0_0_2"]`,
  `routing_unique_lane_signature_topk=["0_0_2"]`, and
  `trajectory_path_length_p95_m≈29.67`. This narrows the export gap further:
  Planning's sampled trajectory shape is visible and same-lane-window locally,
  but `reference_line_claim_grade_allowed=false`; the next evidence step is
  still to export/decode Planning reference-line debug and Control station
  semantics in one frame rather than tune actuation.
- The latest analyzer refresh also compares Control `simple_lat` target point
  coordinates against the same-row Planning sampled trajectory polyline. On the
  same cut-in run,
  `control_target_point_vs_planning_trajectory_sample.classification=control_target_point_on_planning_trajectory_sample_line_candidate`,
  `sample_coverage_ratio≈0.741`, and
  `target_point_to_planning_sample_line_abs_p95_m≈2.05e-8`. This is useful
  same-frame diagnostic evidence that Control's local target point lies on the
  sampled Planning trajectory when both fields are present. It remains
  `reference_line_claim_grade_allowed=false` and cannot replace exported
  Planning reference-line debug; Phase 1 therefore remains `PARTIAL`, with the
  representative Apollo sample still `failed/lane_invasion`.
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
- External/package-level completion review must be built from the generated
  catalog plus the exact accepted bundles referenced by `accepted_bundle_path`.
  Use:

```bash
python tools/package_phase1_review_pack.py \
  --catalog artifacts/phase1_scenario_catalog_current/phase1_scenario_catalog.json \
  --out artifacts/phase1_completion_review_pack \
  --archive
```

  The resulting `phase1_review_pack_manifest.json` is the package-level audit
  surface. It records copied accepted bundles, exact Apollo/PlanningControl run
  IDs, SHA256 hashes, missing required evidence, and scenario blockers. A
  review pack with `status=PARTIAL` cannot support a Phase 1 completion claim,
  even if the local repository catalog still shows `8 DONE / 0 PARTIAL`.

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

### Phase 1.1 Backlog Scenarios

These scenarios have useful scaffolding and accepted comparison-surface
artifacts, but they are no longer Phase 1 delivery blockers under the
delivery-first scope. Keep their artifacts for regression and future review,
but do not let them delay the five P0 delivery scenarios above.

1. `cut_out_simple`
   - Lead vehicle cuts out.
   - Inspects target transition and speed recovery.
2. `junction_turn_no_signal`
   - Unsignalized junction / turn passing.
3. `lead_hard_brake`
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
| scenario case catalog | Enumerate reusable ScenarioCases | PARTIAL | `carla_testbed/analysis/phase1_scenario_catalog.py`; `tools/phase1_scenario_catalog.py`; `carla_testbed/analysis/phase1_acceptance.py`; `tools/phase1_acceptance.py`; `artifacts/phase1_scenario_catalog_current/phase1_scenario_catalog.json`; `tests/test_phase1_scenario_catalog.py`; `tests/test_phase1_acceptance.py` | The local catalog can still report `8 DONE / 0 PARTIAL` as an accepted comparison-surface artifact state, but the latest Pro audit requires the project progress table to treat this as `PARTIAL` until an external package includes the generated catalog, exact comparison directories, participating runs, raw traces, artifact-completeness reports, assist ledgers, and hashes needed to independently recompute the result. This is Phase 1 comparison-surface evidence, not Apollo natural-driving success. | Keep accepted bundles, but make each external review pack self-contained before using catalog DONE language outside the local machine. |
| follow_stop_static | P0 static lead stop | PARTIAL | `configs/scenarios/baguang/follow_stop_static_300m.yaml`; `configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml`; `configs/scenario_templates/static_lead_stop.yaml`; `runs/phase1_acceptance_follow_stop_static_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py`; `tests/test_phase1_scenario_catalog.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py` | Local accepted comparison bundle exists and includes target actor traces, phase events, v-t-gap, comparison, acceptance, and control trace surfaces. The representative Apollo run is still an evaluable behavior failure, so this is not Apollo follow-stop success and remains Phase 1 `PARTIAL` at project-progress level. | Use accepted evidence to prioritize behavior blocker reduction rather than rebuilding the comparison surface. |
| lead_decel_accel | P0 lead speed-change following | PARTIAL | `configs/scenario_templates/lead_vehicle_accel_decel.yaml`; `configs/scenarios/baguang/lead_decel_accel_70_40_70_20m.yaml`; `configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml`; `configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml`; `runs/phase1_acceptance_lead_decel_accel_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py`; `tests/test_v_t_gap_extractor.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py` | The P0 matrix now uses the combined decelerate-then-accelerate Baguang scene, so the canonical case name matches the actual target speed profile. Local accepted comparison bundle evidence still comes from earlier lead-speed-change artifacts; participating Apollo runs remain evaluable failures, so this is comparison-surface completion only and Phase 1 project progress remains `PARTIAL`. | Re-run the P0 matrix with the combined scenario and reduce representative Apollo/Baguang lane-invasion blockers. |
| cut_in_simple | P0 target enters ego lane | PARTIAL | `configs/scenario_templates/cut_in.yaml`; `configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml`; `configs/scenarios/town01/cut_in_097.yaml`; `runs/phase1_apollo_sidecar_cut_in_bvar_prediction_online_20260620_181700/artifacts/apollo_planning.data`; `runs/phase1_apollo_sidecar_cut_in_bvar_prediction_online_20260620_181700/analysis/prediction_evidence/prediction_evidence_report.json`; `runs/phase1_apollo_sidecar_cut_in_event_append_online_20260620_195742/events.jsonl`; `runs/phase1_apollo_sidecar_cut_in_event_append_online_20260620_195742/analysis/baguang_lane_event_contract/baguang_lane_event_contract_report.json`; `runs/phase1_builtin_baguang_cut_in_safety_attach_online_20260620_203218/summary.json`; `runs/phase1_comparisons/baguang_cut_in_time_aligned_v_t_gap_20260620_204023/comparison_summary.json`; `runs/phase1_acceptance_cut_in_simple_time_aligned_v_t_gap_20260620_204023/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_player.py::test_baguang_cut_in_starts_at_configured_activation_gap`; `tests/test_apollo_prediction_evidence.py::test_planning_bvar_prediction_count_counts_as_native_observed_with_source`; `tests/test_scenario_comparison.py::test_lane_event_contract_blocks_lane_invasion_backend_loss`; `tests/test_baguang_lane_event_contract.py::test_downstream_lane_invasion_before_footprint_crossing_is_quarantined`; `tests/test_baguang_lane_event_contract.py::test_yaw_aware_vehicle_footprint_prevents_false_quarantine`; `tests/test_v_t_gap_extractor.py::test_v_t_gap_aligns_relative_actor_trace_to_absolute_apollo_timeseries`; `tests/unit/test_apollo_compat_runtime.py::test_transition_events_append_without_erasing_backend_events`; `tests/test_builtin_ego_runner_safety_integration.py`; `tests/test_phase1_acceptance.py` | The newest correctly configured Apollo cut-in sample preserves root `events.jsonl` runtime events and includes `crossed_lane_marking_types=["Solid"]`; scenario actor contract passes and `v_t_gap` is available. After yaw-aware vehicle footprint correction, the Apollo lane event is hard-gate eligible and classified as `possible_real_lane_departure_or_unclassified_lane_event`. The refreshed builtin run attaches safety sensors after ego spawn settling, has `lane_invasion_count=0`, and clears the previous low-CTE quarantine. The local comparison surface is accepted, but Apollo's representative run still failed with lane invasion and external review must receive raw traces to independently recompute it. | Use this accepted bundle to reduce Apollo cut-in behavior blockers; do not reinterpret it as Apollo cut-in success. |
| lane_keep_straight | P0 lateral baseline | PARTIAL | `configs/scenarios/town01/lane_keep_097.yaml`; `runs/phase1_acceptance_lane_keep_straight_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_scenario_catalog.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Local route-only comparison-surface evidence is self-contained and accepted, including executable control trace surfaces. Apollo's participating run remains an evaluable failure, not natural-driving capability evidence. | Use this accepted route-only row as the template for future review packs. |
| lane_keep_curve | P0 lateral curve baseline | PARTIAL | `configs/scenarios/town01/curve217_diagnostic.yaml`; `runs/phase1_acceptance_lane_keep_curve_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_status_classifier.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Local route-only curve comparison-surface evidence is self-contained and accepted. Apollo behavior remains diagnosable with route/reference-line style blockers; do not tune control from this result alone. | Use this surface for route/reference-line blocker reduction. |
| cut_out_simple | Phase 1.1 backlog target exits ego lane | PARTIAL | `configs/scenario_templates/cut_out.yaml`; `configs/scenarios/baguang/cut_out_35kph_right_to_left_25m.yaml`; `configs/scenarios/town01/cut_out_097.yaml`; `runs/phase1_acceptance_cut_out_simple_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py::test_baguang_cut_out_compiles_lane_change_phase`; `tests/test_phase1_acceptance.py`; `tests/test_apollo_fixed_scene_launch_plan.py::test_apollo_baguang_cut_out_launch_plan_uses_sidecar_runtime_command` | Local accepted comparison bundle exists with lane-change actor/phase evidence and control trace surfaces. This is useful regression evidence, but under the delivery-first scope it is no longer a Phase 1 completion blocker. | Keep lane-change playback evidence separate from ego autonomy success and use it for Phase 1.1 blocker triage. |
| junction_turn_no_signal | Phase 1.1 backlog junction/turn case | PARTIAL | `configs/scenarios/town01/junction_031.yaml`; `runs/phase1_acceptance_junction_turn_no_signal_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_scenario_binding.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Local route-only junction comparison-surface evidence is self-contained and accepted. This no-signal row remains useful but is not part of the five-scenario Phase 1 delivery gate. | Move behavior work to Phase 1.1 after the five P0 delivery scenarios are runnable through the unified pair runner. |
| lead_hard_brake | Phase 1.1 backlog hard-brake following | PARTIAL | `configs/scenarios/baguang/lead_hard_brake_70_to_0_20m.yaml`; `runs/phase1_acceptance_lead_hard_brake_selfcontained_control_20260620_185256/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py::test_baguang_lead_hard_brake_profile_matches_70_to_stop`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py` | Local accepted comparison bundle exists with target actor, phase, v-t-gap, and control trace evidence. It is now Phase 1.1 backlog evidence, not a Phase 1 delivery blocker. | Preserve the accepted bundle and return after P0 runtime delivery is stable. |
| ApolloBackend | Reference external AD-stack backend | PARTIAL | `carla_testbed/backends/apollo_cyberrt.py`; `configs/platforms/apollo_cyberrt.yaml`; `configs/algorithms/apollo/apollo10_carla_gt.yaml`; `tools/run_phase1_scenario.py`; `runs/phase1_apollo_followstop_obstacle_type_metadata_20260618_012159/analysis/phase1_status/phase1_status.json`; `tests/test_platform_backends.py`; `tests/test_apollo_fixed_scene_launch_plan.py`; `tests/test_phase1_status_classifier.py` | Facade and bridge evidence exist, fixed-scene preflight can write structured `backend_not_ready` artifacts, and one legacy Apollo follow-stop compatibility run is now evaluable as `failed/unsafe_gap` rather than invalid target/setup evidence. Actual fixed-scene Apollo runtime dispatch is still not migrated behind the facade, and this is not Apollo natural-driving success. | Migrate the compatibility path behind the ApolloBackend facade and compare the same ScenarioCase against `PlanningControlBackend` without weakening invalid-run semantics. |
| PlanningControlBackend | Self-written planning/control baseline backend | PARTIAL | `carla_testbed/backends/carla_builtin.py`; `configs/platforms/carla_builtin.yaml`; `configs/algorithms/builtin/simple_acc_route_follower.yaml`; `carla_testbed/control/simple_acc_route_follower.py`; `carla_testbed/scenario_player/builtin_ego_runner.py`; `runs/phase1_builtin_follow_stop_097_global_route_20260618_030049/analysis/phase1_status/phase1_status.json`; `runs/phase1_builtin_lead_accel_decel_097_goal_cycle_20260618_031551/analysis/phase1_status/phase1_status.json`; `runs/phase1_builtin_baguang_lead_decel_safety_surface_20260619_210418/analysis/phase1_status/phase1_status.json`; `tests/test_builtin_ego_controller.py`; `tests/test_manifest_input_contract.py` | Current runtime name remains `carla_builtin`; manifests identify it with `backend_type=planning_control_backend`, `input_contract=scene_truth_direct`, and diagnostic-only claim boundaries. It supports fixed-scene and route-only Phase 1 diagnostic runs. The Town01 `lead_decel_accel` builtin run remains evaluable success, while the refreshed Baguang lead-decel safety-surface run is evaluable but failed with near-start `lane_invasion`; both are diagnostic baseline evidence, not Apollo/Autoware capability, and any Apollo/Autoware comparison must use the same acceptance gate. | Use these manifest fields in comparison reports; do not rename runtime modules for documentation consistency, and do not count builtin success or failure as Apollo/Autoware capability. |
| BackendRuntimeAdapter / ScenarioRunExecutor | Execute one resolved RunPlan through backend lifecycle | PARTIAL | `carla_testbed/platform/runtime_adapter.py`; `carla_testbed/platform/executor.py`; `python3 -m carla_testbed run --plan <plan> --run-dir <run>`; `tests/test_backend_runtime_adapter.py`; `tests/test_scenario_run_executor.py`; `tests/test_platform_executor.py` | The CLI no longer stops at "runtime dispatch not implemented"; it records PID, stdout/stderr, timeout, postprocess outcome, and platform execution result. This proves the execution surface exists, not that a given Apollo online run succeeds. | Use this entry for the next online P0 runs and keep behavior failures in `phase1_status` / `ScenarioComparison`. |
| Phase 1 pair runner | One-command ApolloBackend vs PlanningControlBackend pair | PARTIAL | `carla_testbed/platform/phase1_pair_runner.py`; `python3 -m carla_testbed phase1 run-pair --scenario <case> --out <dir>`; `tests/test_phase1_pair_runner.py` | The pair runner compiles both plans, executes them sequentially, and writes ScenarioComparison. Dry-run CI coverage exists; online Apollo success remains unproven and invalid runs remain invalid/partially evaluable. | Run the five P0 scenarios through this entry online and reduce the first Apollo behavior blocker instead of adding more acceptance layers. |
| Phase 1 P0 matrix runner | One-command orchestration for the five P0 pairs | PARTIAL | `carla_testbed/platform/phase1_p0_matrix.py`; `python3 -m carla_testbed phase1 run-p0-matrix --out <dir>`; `tests/test_phase1_p0_matrix.py`; `runs/phase1_p0_matrix/phase1_p0_online_pair_isolated_20260625_143833`; `runs/phase1_p0_matrix/phase1_p0_online_pair_isolated_20260625_143833/analysis/phase1_posthoc_artifact_normalization/summary.json` | The matrix runner materializes `follow_stop_static`, `lead_decel_accel`, `cut_in_simple`, `lane_keep_straight`, and `lane_keep_curve` through the same `run-pair` surface, writing `phase1_p0_matrix_manifest.json` and CSV. Pair-isolated online evidence has all five rows materialized. Post-hoc normalization proves both Town01 Apollo route-only rows contained unique nested `timeseries.csv`, `events.jsonl`, and `control_apply_trace.jsonl`; after writing route-only `v_t_gap=not_applicable` and Phase 1 artifact-completeness reports, the existing matrix recomputes to `5 comparable` and `5 apollo_vs_planning_control_evaluable` pairs. That local result was not independently included in the reviewed package, and Apollo still behavior-fails in every row (`lane_invasion` or `timeout`), so this is platform comparability evidence, not Apollo success. | Keep pair-isolated `--start-carla` as the delivery entry. Next highest-value work is to rerun the full P0 matrix with the automatic normalization/comparison-artifact path and package the exact five-pair run evidence before making external completion claims. |
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
- The Baguang combined lead-decelerate/accelerate Apollo pair now has an
  evaluable initial-state synchronization sequence. In
  `runs/phase1_online_pairs/phase1_p0_lead_decel_accel_joint_initial_state_settle_v7_20260711/`,
  the fixed-scene hook first restores the canonical ego pose, observes the ego
  at the declared initial speed for 10 ticks, then spawns the lead and observes
  both actors at 19.44m/s for another 10 ticks before scene time starts. The
  run records `initial_speed_materialization_count=20`,
  `role_speed_ready_count=10`, and a passing scenario actor contract. This
  removes the v6 startup artifact where the lead appeared at 0.49m/s for the
  first scene frame while the ego was already at 19.45m/s. The v7 Apollo run is
  evaluable Phase 1 `success`, with no collision/lane-invasion failure, but it
  is not natural-driving evidence: pre-scene state materialization is explicit
  scenario setup and stops at scene time zero.
- The same v7 evidence exposes the next longitudinal contract issue rather
  than proving natural following quality. `s_offset_m=20.0` places actor
  centers about 19.03m apart, while bbox-aware `v-t-gap` reports only 14.14m
  bumper clearance at 70kph. Apollo still brakes sharply during the first
  second and the gap grows to 77.28m before ending at 57.37m. The next change
  must define whether scenario `initial_gap_m=20.0` means center distance or
  bumper clearance and make spawn feasibility verify that declaration; it
  must not tune Apollo longitudinal control around an ambiguous setup.
- That ambiguity is closed by the v8 pair at
  `runs/phase1_online_pairs/phase1_p0_lead_decel_accel_bumper_gap_v8_20260711/`.
  The scenario now declares `gap_reference=bumper_to_bumper`; runtime bbox
  feasibility measures 20.008m against the requested 20.0m and passes. Both
  actors start the scene at about 19.44m/s. Apollo remains evaluable but
  `degraded/large_final_gap`: it slows to 3.50m/s during the first second and
  ends at 66.99m gap. The builtin baseline also slows for the roughly 1.03s
  initial headway, but ends at 22.29m. This comparison moves the primary
  blocker into Apollo longitudinal response to a short-headway lead; it does
  not justify relaxing the scenario evidence contract or changing the frozen
  lateral configuration.
- The follow-up v9 pair at
  `runs/phase1_online_pairs/phase1_p0_lead_decel_accel_atomic_target_v9_20260711/`
  removes a second setup confounder. A moving lead is now given its initial
  velocity before CARLA's internal spawn-materialization tick and the scene is
  armed immediately once that observed velocity is valid. Apollo no longer
  sees the lead while the setup hook pins ego speed: there is no pre-scene raw
  brake or `SPEED_FALLBACK`, and the ego remains at about 19.5m/s through the
  first 0.2s. Planning first enters `SPEED_FALLBACK` around scene time 0.25s
  after consuming the real lead observation. The ego is still at 10.83m/s at
  1.1s rather than v8's 3.57m/s, but the run remains evaluable
  `degraded/large_final_gap` with 69.53m final gap. The remaining blocker is
  Apollo's longitudinal planning response, not target spawn speed, gap
  materialization, bridge guard behavior, or the frozen lateral configuration.
- The scenario-first v10/v11/v13 sequence isolates the remaining startup
  boundary. In v10, the complete fixed scene exists before Apollo starts, but
  eager Control publishes its default brake while Planning is still empty;
  the run avoids collision only after that startup brake enlarges the gap. v11
  delays the complete Control module and collides, proving that subscriber
  startup must remain eager. The retained implementation therefore leaves
  Control subscribed and adds a one-shot bridge publication latch: raw Apollo
  Control remains recorded, CARLA publication waits for the first valid
  Planning trajectory, and the latch never closes on later fallback.
- The online v13 pair at
  `runs/phase1_online_pairs/phase1_p0_lead_decel_accel_scenario_first_publish_gate_v13_20260711/`
  proves that latch is materialized at runtime (`skip_count=875`, then open),
  preserves the 20.008m initial bumper gap, and removes startup default braking
  as a hidden gap-building assist. It also exposes a real evaluable Apollo
  behavior failure: Planning produces only 1 non-empty message out of 83,
  CARLA publication opens after the lead's five-second deceleration trigger,
  and the run collides with final bumper gap about -0.02m. This is not an
  improvement claim; it moves the blocker to Apollo readiness versus dynamic
  scenario activation and subsequent planning continuity.
- v14/v15 tested but rejected a continuous-speed readiness preroll. v14 waited
  on a stats artifact that is only finalized during cleanup and never armed;
  v15 used the live control log and armed correctly, but the warmup consumed
  roughly 170m of road before scene time zero, leaving only about 5.4s of the
  intended 18s profile. That preroll is not retained as the default. The next
  lifecycle design must establish backend readiness without consuming formal
  scenario distance or presenting physically inconsistent pose/velocity, then
  revalidate the same pair without changing Planning, Control, longitudinal
  mapping, or the frozen lateral configuration.

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

The original evidence-spine PR sequence has largely landed. Under the
2026-06-25 delivery-first scope, do not keep extending acceptance schemas unless
they block a real valid run. The next PR sequence is:

1. Use the hardened `ScenarioRunExecutor` lifecycle in online runs and verify
   that runtime failures still produce `phase1_status` / failure-reason
   postprocess artifacts.
2. Produce one canonical `follow_stop_static` Apollo run with
   `blocking_assists=[]`; behavior failure is acceptable, but assisted success
   is not accepted ApolloBackend ownership evidence.
3. Run the `follow_stop_static` pair through
   `python3 -m carla_testbed phase1 run-pair --scenario <case> --out <dir>`
   and confirm both runs are evaluable rather than setup/config/artifact
   invalid.
4. Dry-run the five-row matrix with
   `python3 -m carla_testbed phase1 run-p0-matrix --out <dir> --dry-run` and
   verify each row materializes both backend manifests with the backend
   contract and claim boundary fields.
5. Run the same matrix online. `lead_decel_accel`, `cut_in_simple`,
   `lane_keep_straight`, and `lane_keep_curve` should preserve invalid-run
   boundaries and avoid new acceptance/schema work; route/reference-line
   blockers should be reported as behavior evidence rather than schema gaps.
6. Rebuild the Phase 1 review pack from the exact matrix/pair outputs,
   timeseries, actor/phase/control traces, assist ledgers, and comparison
   artifacts.

Historical sequence kept for provenance:

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
