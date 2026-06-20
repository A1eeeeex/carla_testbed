# Phase 1 Engineering Brief

Last reviewed: 2026-06-20

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

Latest reviewed snapshot: `2026-06-20`, after the latest GPT Pro audit of the
accepted-bundle evidence.

Current catalog status using `tools/phase1_scenario_catalog.py --repo .
--evidence-root runs`:

- Total scenarios tracked: `8`
- Accepted `DONE`: `0`
- `PARTIAL`: `8`
- `NOT_YET`: `0`
- `UNKNOWN`: `0`

Interpretation:

- The Phase 1 platform skeleton is substantially built: scenario definitions,
  target actor contracts, `v-t-gap`, status classification, ScenarioComparison,
  and Apollo sidecar evidence all exist.
- The latest Pro audit found that the previous `8/8 DONE` statement was still
  too optimistic. The available accepted-bundle outputs are useful mechanism
  evidence, but the review package did not independently preserve enough raw
  artifacts to re-compute all comparisons and found stale/invalid surfaces in
  representative rows.
- `cut_in_simple` is the clearest blocker: the latest refreshed Apollo run now
  starts inside the cut-in activation window, but both participating backends
  still fail with `lane_invasion`, and Apollo link health is blocked by
  `prediction_evidence:closed_loop`.
- The next Phase 1 milestone is to convert one representative row at a time
  into a self-contained, re-computable accepted bundle with raw `timeseries.*`,
  actor trace, phase events, `v_t_gap.csv`, comparison summary, acceptance
  report, and no stale evidence. Until that exists for the catalog, Phase 1
  remains `PARTIAL`.
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
| scenario case catalog | Enumerate reusable ScenarioCases | PARTIAL | `carla_testbed/analysis/phase1_scenario_catalog.py`; `tools/phase1_scenario_catalog.py`; `carla_testbed/analysis/phase1_acceptance.py`; `tools/phase1_acceptance.py`; `configs/scenarios/`; `configs/scenario_templates/`; `runs/phase1_acceptance_cut_in_simple_20260620_170727/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_scenario_catalog.py`; `tests/test_phase1_acceptance.py` | Catalog and acceptance mechanics exist, but the latest Pro audit found the current evidence package still cannot independently re-compute all accepted rows and still contains stale/invalid surfaces. The safe status is `PARTIAL` until accepted bundles are self-contained and review-pack complete. | Keep the catalog report in review packs; require raw run artifacts and re-computable accepted bundles before restoring any scenario row to DONE. |
| follow_stop_static | P0 static lead stop | PARTIAL | `configs/scenarios/baguang/follow_stop_static_300m.yaml`; `configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml`; `configs/scenarios/town01/follow_stop_097.yaml`; `configs/scenario_templates/static_lead_stop.yaml`; `configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_compat.yaml`; `runs/phase1_acceptance_follow_stop_static_20260620_154807/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py`; `tests/test_phase1_scenario_catalog.py`; `tests/test_baguang_lane_event_contract.py`; `tests/test_scenario_comparison.py`; `tests/test_apollo_fixed_scene_launch_plan.py`; `tests/test_phase1_acceptance.py`; `tests/test_phase1_run_postprocess.py` | Mechanism evidence exists, but the accepted bundle is not yet enough to mark the row complete under the latest external audit because raw artifacts and independent recomputation evidence remain incomplete. The Apollo run remains an evaluable `lane_invasion` / downstream blocker failure. | Rebuild a self-contained accepted bundle with raw traces, v-t-gap CSV, comparison, acceptance report, and no stale evidence. |
| lead_decel_accel | P0 lead speed-change following | PARTIAL | `configs/scenario_templates/lead_vehicle_accel_decel.yaml`; `configs/scenarios/town01/lead_accel_decel_097.yaml`; `configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml`; `configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml`; `runs/phase1_acceptance_lead_decel_accel_20260620_155835/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py`; `tests/test_v_t_gap_extractor.py`; `tests/test_phase1_run_postprocess.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py` | Scenario and analyzer surfaces exist, but participating runs are still evaluable `lane_invasion` failures and the review evidence is not self-contained enough for DONE. | Re-run/postprocess as a complete bundle and preserve raw actor/phase/control/timeseries evidence. |
| cut_in_simple | P0 target enters ego lane | PARTIAL | `configs/scenario_templates/cut_in.yaml`; `configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml`; `configs/scenarios/town01/cut_in_097.yaml`; `runs/phase1_apollo_sidecar_cut_in_target_evaluable_online_20260620_165920/analysis/prediction_evidence/prediction_evidence_report.json`; `runs/phase1_acceptance_cut_in_simple_20260620_170727/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_player.py::test_baguang_cut_in_starts_at_configured_activation_gap`; `tests/test_scenario_actor_contract.py::test_lane_change_contract_uses_actor_world_motion_when_ego_relative_lateral_drifts`; `tests/test_phase1_status_classifier.py::test_duration_policy_not_reached_after_target_interaction_is_evaluable_failure`; `tests/test_apollo_prediction_evidence.py::test_dynamic_case_with_prediction_runtime_but_no_output_fails_explicitly`; `tests/test_apollo_link_health.py::test_dynamic_prediction_placeholder_is_regenerated_and_written_before_consumption` | Target activation is now exercised, but both backends remain evaluable `lane_invasion` failures and Apollo is blocked by `prediction_evidence:closed_loop`. This is blocker evidence, not completion. | Capture Apollo Prediction logs/output in the next online run and keep cut-in target-transition evidence self-contained. |
| lane_keep_straight | P0 lateral baseline | PARTIAL | `configs/scenarios/town01/lane_keep_097.yaml`; `runs/phase1_acceptance_lane_keep_straight_20260620_153947/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_scenario_catalog.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Route-only comparison evidence exists, but the latest audit requires a self-contained review bundle before this can be counted as DONE. Apollo's participating run remains an evaluable failure, not natural-driving pass evidence; any natural-driving pass claim would require `natural_driving_report.json` plus the no-interference artifacts. | Repackage with raw artifacts and authoritative status/comparison surfaces. |
| lane_keep_curve | P0 lateral curve baseline | PARTIAL | `configs/scenarios/town01/curve217_diagnostic.yaml`; `runs/phase1_acceptance_lane_keep_curve_20260620_155918/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_status_classifier.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Accepted-bundle mechanics exist, but Apollo remains evaluable with `route_establishment_latency`/reference-line style blockers, and raw evidence must be preserved for recomputation. | Use this surface for route/reference-line blocker reduction; do not tune control from this result alone. |
| cut_out_simple | P1 target exits ego lane | PARTIAL | `configs/scenario_templates/cut_out.yaml`; `configs/scenarios/town01/cut_out_097.yaml`; `configs/scenarios/baguang/cut_out_35kph_right_to_left_25m.yaml`; `runs/phase1_acceptance_cut_out_simple_20260620_1622/acceptance/phase1_acceptance_report.json`; `carla_testbed/scenario_player/templates/cut_out.py`; `tests/test_fixed_scene_compiler.py::test_baguang_cut_out_compiles_lane_change_phase`; `tests/test_fixed_scene_contract.py::test_lane_change_progress_can_complete_phase_without_stop_event`; `tests/test_phase1_acceptance.py`; `tests/test_apollo_fixed_scene_launch_plan.py::test_apollo_baguang_cut_out_launch_plan_uses_sidecar_runtime_command` | Lane-change trace completion evidence exists, but both participating runs remain evaluable `lane_invasion` failures and the row still needs a review-complete accepted bundle. | Keep lane-change playback evidence separate from ego autonomy success and preserve raw traces in future packs. |
| junction_turn_no_signal | P1 junction/turn case | PARTIAL | `configs/scenarios/town01/junction_031.yaml`; `runs/phase1_acceptance_junction_turn_no_signal_20260620_155921/acceptance/phase1_acceptance_report.json`; `tests/test_phase1_scenario_binding.py`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py`; `tests/test_run_artifact_completeness.py` | Route-only junction comparison evidence exists, but Apollo remains evaluable with runtime/route-establishment blockers and the review bundle is not yet self-contained enough for DONE. | Re-run/postprocess with full artifacts and keep traffic-light/natural-driving claims out of this no-signal row. |
| lead_hard_brake | P1 hard-brake following | PARTIAL | `configs/scenarios/baguang/lead_hard_brake_70_to_0_20m.yaml`; `runs/phase1_acceptance_lead_hard_brake_20260620_155924/acceptance/phase1_acceptance_report.json`; `tests/test_fixed_scene_compiler.py::test_baguang_lead_hard_brake_profile_matches_70_to_stop`; `tests/test_scenario_comparison.py`; `tests/test_phase1_acceptance.py` | Mechanism evidence exists, but both participating runs remain evaluable `lane_invasion` failures and Apollo link-health still reports runtime/module blockers. | Preserve raw v-t-gap/actor/control evidence and reduce representative Apollo blockers before claiming completion. |
| ApolloBackend | Reference external AD-stack backend | PARTIAL | `carla_testbed/backends/apollo_cyberrt.py`; `configs/platforms/apollo_cyberrt.yaml`; `configs/algorithms/apollo/apollo10_carla_gt.yaml`; `tools/run_phase1_scenario.py`; `runs/phase1_apollo_followstop_obstacle_type_metadata_20260618_012159/analysis/phase1_status/phase1_status.json`; `tests/test_platform_backends.py`; `tests/test_apollo_fixed_scene_launch_plan.py`; `tests/test_phase1_status_classifier.py` | Facade and bridge evidence exist, fixed-scene preflight can write structured `backend_not_ready` artifacts, and one legacy Apollo follow-stop compatibility run is now evaluable as `failed/unsafe_gap` rather than invalid target/setup evidence. Actual fixed-scene Apollo runtime dispatch is still not migrated behind the facade, and this is not Apollo natural-driving success. | Migrate the compatibility path behind the ApolloBackend facade and compare the same ScenarioCase against `PlanningControlBackend` without weakening invalid-run semantics. |
| PlanningControlBackend | Self-written planning/control baseline backend | PARTIAL | `carla_testbed/backends/carla_builtin.py`; `configs/platforms/carla_builtin.yaml`; `configs/algorithms/builtin/simple_acc_route_follower.yaml`; `carla_testbed/control/simple_acc_route_follower.py`; `carla_testbed/scenario_player/builtin_ego_runner.py`; `runs/phase1_builtin_follow_stop_097_global_route_20260618_030049/analysis/phase1_status/phase1_status.json`; `runs/phase1_builtin_lead_accel_decel_097_goal_cycle_20260618_031551/analysis/phase1_status/phase1_status.json`; `runs/phase1_builtin_baguang_lead_decel_safety_surface_20260619_210418/analysis/phase1_status/phase1_status.json`; `tests/test_builtin_ego_controller.py`; `tests/test_manifest_input_contract.py` | Current runtime name remains `carla_builtin`; manifests identify it with `backend_type=planning_control_backend`, `input_contract=scene_truth_direct`, and diagnostic-only claim boundaries. It supports fixed-scene and route-only Phase 1 diagnostic runs. The Town01 `lead_decel_accel` builtin run remains evaluable success, while the refreshed Baguang lead-decel safety-surface run is evaluable but failed with near-start `lane_invasion`; both are diagnostic baseline evidence, not Apollo/Autoware capability, and any Apollo/Autoware comparison must use the same acceptance gate. | Use these manifest fields in comparison reports; do not rename runtime modules for documentation consistency, and do not count builtin success or failure as Apollo/Autoware capability. |
| ScenarioRun artifact structure | Per-run artifact envelope | PARTIAL | `docs/run_artifacts.md`; `carla_testbed/record/artifact_store.py`; `tests/unit/recording/test_artifact_store.py`; `carla_testbed/scenario_player/builtin_ego_runner.py` | Existing paths differ from the recommended Phase 1 layout; some online tools still write richer legacy artifacts. | Add a non-invasive artifact completeness checker for Phase 1 ScenarioRun. |
| ScenarioComparison artifact structure | Backend comparison envelope | PARTIAL | `carla_testbed/analysis/scenario_comparison.py`; `tools/compare_scenario_runs.py`; `runs/phase1_comparisons/lane_keep_real_apollo_vs_builtin_safety_surface_20260619_164004/comparison_summary.json`; `runs/phase1_comparisons/follow_stop_097_apollo_vs_builtin_20260618_030308/comparison_summary.json`; `runs/phase1_comparisons/baguang_lead_decel_apollo_overlay_vs_builtin_safety_surface_20260619_210445/comparison_summary.json`; `runs/phase1_comparisons/baguang_lead_hard_brake_apollo_overlay_vs_builtin_safety_surface_20260620_004757/comparison_summary.json`; `runs/phase1_comparisons/baguang_cut_in_apollo_overlay_vs_builtin_safety_surface_20260620_005828/comparison_summary.json`; `runs/phase1_comparisons/baguang_cut_out_apollo_overlay_vs_builtin_safety_surface_20260620_010954/comparison_summary.json`; `tests/test_scenario_comparison.py`; Baguang-specific wrapper in `carla_testbed/analysis/baguang_stack_comparison.py` | Generic comparison exists, preserves invalid-run boundaries, records `comparison_target_status` / `backend_coverage`, checks safety-event evidence-surface consistency for `lane_invasion` and `collision`, and now has comparable evidence for all current P0/P1 rows. It is still PARTIAL as an artifact envelope because schemas and plotting surfaces remain minimal. | Keep extending report detail without weakening invalid-run or evidence-surface semantics. |
| v-t-gap extraction | Core longitudinal behavior curves | PARTIAL | `carla_testbed/analysis/v_t_gap.py`; `tools/extract_v_t_gap.py`; `runs/phase1_online_builtin_lane_keep_20260617_135010/analysis/v_t_gap/v_t_gap_report.json`; `runs/phase1_builtin_lead_accel_decel_097_goal_cycle_20260618_031551/analysis/v_t_gap/v_t_gap_report.json`; `tests/test_v_t_gap_extractor.py` | Extractor supports `timeseries.csv/jsonl` plus actor traces, filters cut-in target activation, emits `not_applicable` for route-only cases, uses same-lane `route_s_bumper_gap` when route/lane evidence is comparable, rejects route-s rows that conflict with the actor-trace forward anchor, and uses `trajectory_progress_bumper_gap` for fixed-scene lane/road transitions or route-s anchor conflicts. Lane-keep comparison uses the route-only boundary; target-actor scenarios still need Apollo evaluable runs and HDMap/Frenet evidence for natural-driving claims. | Add evaluable ApolloBackend runs for fixed-scene target scenarios and keep trajectory-progress gap clearly separated from Apollo HDMap/Frenet claim evidence. |
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
  `carla_builtin` evidence, a comparable ApolloBackend-vs-PlanningControlBackend
  report, and an accepted `phase1_acceptance_report.json` bundle. The
  scenario starts inside the target activation window so the target transition
  is exercised before the representative Apollo run fails with lane invasion.
  Refreshed link-health evidence narrows the prediction blocker to Prediction
  runtime observed but no `/apollo/prediction` channel output. This is accepted
  Phase 1 comparison-surface evidence, not Apollo cut-in behavior success.
- `cut_out_simple` now has a Baguang fixed-scene ScenarioCase,
  `configs/scenarios/baguang/cut_out_35kph_right_to_left_25m.yaml`, online
  Apollo sidecar evidence, online `carla_builtin` evidence, a comparable
  ApolloBackend-vs-PlanningControlBackend report, and an accepted
  `phase1_acceptance_report.json` bundle. Its representative runs are
  evaluable failures, so this is accepted Phase 1 comparison-surface evidence,
  not a cut-out behavior pass.
- Current catalog completion is governed by accepted bundles, not by the old
  cross-run readiness snapshot. After the latest Pro audit, current P0/P1
  catalog progress is treated as `0 DONE / 8 PARTIAL` until each row has a
  self-contained, independently re-computable accepted bundle in the review
  package.
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
  not Apollo/Autoware capability evidence and do not make Phase 1 DONE.
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
