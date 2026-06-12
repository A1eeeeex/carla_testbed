# Apollo Town01 Truth-Input Natural Driving

This document defines the short-term target for Apollo in CARLA Town01 under
truth-input simulation. The target is healthy, natural route-level driving in
Town01, including lane keeping, curve diagnosis, junction turns, and traffic
light interaction.

Machine-readable suite:

- `configs/scenarios/town01_natural_driving_suite.yaml`
- schema: `natural_driving_suite.v1`
- loader: `carla_testbed.experiments.natural_driving_schema.load_natural_driving_suite`

Traffic-light GT mapping contract:

- `configs/town01/traffic_lights.example.yaml`
- adapter: `carla_testbed.adapters.apollo.traffic_light_gt`
- channel: `/apollo/perception/traffic_light`

Natural-driving channel health contract:

- `configs/algorithms/apollo_natural_driving_channels.yaml`
- analyzer: `carla_testbed.analysis.apollo_channel_health`
- report: `apollo_channel_health_report.json`

Apollo Control handoff evidence:

- analyzer: `carla_testbed.analysis.apollo_control_handoff`
- report: `apollo_control_handoff_report.json`
- row-level trace: `artifacts/control_apply_trace.jsonl` is preferred for
  Apollo raw -> bridge mapped -> CARLA applied -> vehicle response attribution;
  `control_decode_debug.jsonl` / `bridge_control_decode.jsonl` are fallback
  raw/mapped decode evidence only.
- gate: Control process, `/apollo/control`, bridge receive, raw decode, mapped/applied control, and vehicle response must be non-blocking for hard natural-driving pass in `natural_driving_report.json`.

Apollo reference-line contract evidence:

- analyzer: `carla_testbed.analysis.apollo_reference_line_contract`
- report: `apollo_reference_line_contract_report.json`
- gate: lane-keep, curve, junction, and traffic-light hard passes require
  explicit Apollo reference-line evidence with no blocking reasons. The report
  separates `contracts.planning_trajectory`,
  `contracts.control_reference`, and `contracts.apollo_hdmap_projection`.
  Planning trajectory and control reference can pass independently, but a
  claim-grade lane/reference-line pass also requires Apollo HDMap API projection
  evidence. Route heading agreement or bridge nearest-lane diagnostics are
  useful context, but they cannot replace `apollo_hdmap_projection.jsonl`.

Apollo HDMap projection evidence:

- analyzer: `carla_testbed.analysis.apollo_hdmap_projection`
- report: `apollo_hdmap_projection_report.json`
- gate: this report is optional only in diagnostic/smoke mode. A claim-grade
  lane/reference-line pass requires official `source="apollo_hdmap_api"`
  projection rows with bounded heading and lateral error. Missing projection
  remains `insufficient_data`, not an Apollo behavior failure.

## Scope

Truth-input mode means CARLA provides ground-truth inputs such as ego
localization, chassis feedback, route context, obstacles, and traffic-light
state where available. This is useful for isolating routing/planning/control
behavior, but it does not represent full Apollo perception or localization
reproduction.

The current target is not “Apollo complete natural driving.” It is a gated
engineering target:

- link health: routing, planning, control, and required Apollo channels are
  materialized and aligned;
- geometry health: route-health, lateral error, heading error, and curve
  segments are interpretable; curve diagnostics also require Apollo
  matched-point, target-point, and raw steer semantics to be present;
- behavior health: the ego progresses naturally for the scenario class;
- control health: raw/mapped/applied control and control handoff remain
  diagnosable.

Each `natural_driving_report.json` scenario result includes an `evidence`
object (`natural_driving_evidence.v1`). This is a normalized evidence ledger,
not a replacement for the scenario verdict. It records route-health status,
whether the route can support a hard gate, Apollo channel health,
`localization_contract_report.json` status,
`apollo_reference_line_contract_report.json` status,
`apollo_control_handoff_report.json` status, control attribution status,
traffic-light evidence status, assist ledger status, artifact completeness,
active/blocking/non-blocking assists, `assist_confidence`,
`can_claim_unassisted_natural_driving`, `why_not_claimable`, missing artifacts,
missing fields, and warnings.
Missing evidence must stay explicit, for example
`localization_contract_report.json`,
`apollo_reference_line_contract_report.json`,
`apollo_control_handoff_report.json`, `control_attribution_report.json`,
`assist_ledger`, or
`traffic_light_evidence_report.json`; the evaluator must not infer success
from `summary.json` alone.

## Current Boundaries

- `carla_direct` remains an experimental candidate transport, not the default
  backend.
- `ros2_gt` remains the default baseline unless a specific run config says
  otherwise.
- `carla_direct` short-window positives do not prove curve lateral health; any
  `carla_direct` improvement claim requires `ab_report.json`.
- Calibration is control-actuation evidence only. It is not the first curve
  fix and must not automatically change `steer_scale` or promote physical
  mapping.
- Do not change Apollo parameters, enable physical mapping, or modify the core
  harness as part of this suite definition.
- Assisted behavior is not unassisted natural-driving evidence. If
  `assist_ledger` reports blocking assists such as
  `straight_lane_lateral_stabilizer`, `straight_acc_override`, or
  `terminal_stop_hold`, a run can still be diagnostically useful but cannot be
  claimed as an unassisted Apollo natural-driving pass without
  `natural_driving_report.json`.
- Missing assist evidence is also not proof of no assist. A hard natural-driving
  pass requires explicit assist evidence with no active assists; otherwise the
  scenario result stays `insufficient_data` or diagnostic-only.
- Autoware RViz, rosbag2, and CARLA third-person recordings are operator/demo
  evidence only. Autoware comparison with Apollo must go through the same
  route-health, channel-health, control-health, control-attribution, and
  natural-driving reports. See `docs/autoware_recording.md`.

## Suite Structure

Each scenario entry contains:

- `route_id`
- `scenario_id`
- `scenario_class`
- `map`
- `duration_s`
- `spawn_pose` or `spawn_ref`
- `goal_pose` or `goal_ref`
- `route_ref`
- `gate_role`: `hard_gate`, `diagnostic_gate`, or `informational`
- `required_channels`
- `success_criteria`
- `required_artifacts`
- `traffic_light_expectation` for traffic-light scenarios. It records the
  expected behavior (`red_stop`, `green_go`, or `red_to_green_release`), the
  expected signal state sequence, and the report fields required to judge that
  scenario.

Supported `scenario_class` values:

- `lane_keep`
- `curve_diagnostic`
- `junction_turn`
- `traffic_light_red_stop`
- `traffic_light_green_go`
- `traffic_light_red_to_green_release`

Traffic-light scenarios now use selected Town01 route probes that can enter the
online matrix. They remain `informational` rather than hard gates because the
tracked CARLA-to-Apollo signal/stop-line mapping is still placeholder/warn-level
evidence until validated against local Apollo HDMap artifacts.

## Traffic-Light GT Contract

Apollo Planning does not reason over a CARLA traffic-light actor id directly.
For truth-input traffic-light scenarios, the adapter must map a CARLA
`carla_actor_id` or `carla_landmark_id` to the Apollo HDMap `apollo_signal_id`
and associated `stop_line_id` / `lane_ids`.

## Map / Route / Signal Contract Levels

`tools/check_town01_apollo_contract.py` writes
`town01_apollo_contract_report.json` with schema
`town01_apollo_contract_report.v2`. The report separates schema placeholders
from evidence that was verified against explicit artifacts:

- `route_contract_level`
- `spawn_contract_level`
- `hdmap_contract_level`
- `reference_line_contract_level`
- `signal_contract_level`
- `overall_contract_level`
- `can_claim_lane_keep`
- `can_claim_junction`
- `can_claim_traffic_light`

Contract levels are ordered as:

```text
missing < placeholder < schema_only < route_geometry_available < hdmap_file_present < hdmap_verified < reference_line_verified < signal_overlap_verified
```

Verified levels cannot be inferred from field presence alone. They require
explicit artifacts such as `hdmap_validation_report.json`,
`reference_line_validation_report.json`, or `signal_overlap_report.json`.
Placeholder fields never become verified evidence. For example, the tracked
`configs/town01/apollo_contract.example.yaml` is useful for schema checks, but
it cannot support traffic-light or junction claims until local Apollo HDMap,
reference-line, and signal-overlap reports are attached.

Gate interpretation:

- lane-keep claims require at least route geometry plus spawn alignment;
- curve and junction claims require `route_health.json` plus reference-line
  verification unless the run is explicitly diagnostic-only;
- traffic-light claims require `signal_overlap_verified`;
- RoadRunner/Baguang conversion metadata can record source/generated map paths,
  hashes, scale, lane count, signal count, and stop-line count, but it is not
  itself a replacement for HDMap/reference-line/signal-overlap validation.

The CI-safe adapter currently builds a dict payload only. It does not import
CyberRT or Apollo protobufs:

- `signal_id`: Apollo HDMap signal id, not the CARLA actor id;
- `color`: one of `RED`, `YELLOW`, `GREEN`, `UNKNOWN`;
- `confidence`;
- `timestamp_sec`;
- `frame_id`;
- `stop_line_id` and `lane_ids` for traceability;
- `warnings` when a mapping is missing, incomplete, or still a placeholder.

Missing mapping is not silently converted into a harmless `UNKNOWN` light.
If the adapter cannot resolve the Apollo signal id, the payload must carry a
warning such as `missing_signal_mapping` or `missing_apollo_signal_id`. A
placeholder mapping is useful for schema development only; it is not valid
behavior evidence until checked against local Apollo HDMap signal overlaps.

The example mapping intentionally uses placeholder ids:

```bash
python -m pytest tests/test_apollo_traffic_light_gt.py -q
```

The offline traffic-light contract report can be generated from:

- `configs/town01/apollo_contract.example.yaml`
- `configs/town01/traffic_lights.example.yaml`

Those example files intentionally contain placeholders, so the generated
`traffic_light_contract_report.json` should be treated as `warn`-level schema
evidence, not promotion-grade proof that a real Apollo HDMap signal overlap is
valid.

## Traffic-Light Behavior Report

Traffic-light behavior requires a separate artifact:
`traffic_light_behavior_report.json`. This report summarizes red-stop,
green-go, and red-to-green release evidence from `summary.json`,
`timeseries.csv/jsonl`, `events.jsonl`, and the traffic-light contract report.

The behavior report can be `pass`, `warn`, `fail`, or `insufficient_data`.
Contract `warn` keeps behavior at `warn` even if the summary fields look good.
Missing metrics such as `red_stop_distance_m`, `green_pass_time_s`, or
`red_to_green_release_time_s` must stay `insufficient_data`. This keeps
traffic-light interaction claims artifact-backed instead of Dreamview-only.
The report also echoes `traffic_light_expectation` from the run manifest when
available. The suite-level natural-driving evaluator treats traffic-light runs
without `traffic_light_expectation.expected_behavior` as `insufficient_data`,
and treats an expectation/scenario mismatch as `fail`. The final Town01 goal
audit applies the same rule and requires the expectation to be manifest-backed;
a stale behavior-report echo alone is not enough for a traffic-light pass.
For claim-grade deterministic probes, the behavior report also echoes and
checks `traffic_light_control` metadata from `summary.json` or `manifest.json`.
The report cannot pass unless the metadata shows `deterministic_gt_control`
actually affected at least one CARLA traffic-light actor; red-to-green probes
also require an observed release event.
Traffic-light expectations also carry `stimulus_mode`. The current suite uses
`deterministic_gt_control` for red-stop, green-go, and red-to-green-release
probes. The runner translates that expectation into explicit
`scenario.traffic_lights.*` overrides so CARLA traffic-light actors are frozen
to the requested state, and red-to-green can release after a configured delay.
This improves reproducibility over `carla_actual_observed`, but it is still
only truth-input stimulus evidence. Claim-grade publication to Apollo must be
separate: `traffic_light_policy=carla_actual`, `color_source` must be
`carla_actor_state`, `carla_landmark_state`, or
`carla_traffic_light_actor_state`, `confidence >= 0.99`, and `contain_lights`
must match the controlled intersection. `force_green` is a smoke/debug input
only and blocks natural-driving claims even if the vehicle moves correctly. A
pass still requires Apollo HDMap signal/stop-line contract evidence,
`/apollo/perception/traffic_light` channel health,
`traffic_light_behavior_report.json`, and actual run metadata showing
`traffic_light_control` affected CARLA traffic-light actors. For red-to-green
release, that metadata must also contain a release event. Deterministic
stimulus alone does not prove traffic-light behavior is solved.
For red-stop scenarios, `red_stop_distance_m` alone is not enough for a pass:
the report also requires explicit `stopped_at_red=true` evidence, or derives it
from timeseries rows that show the ego close to the stop line with speed below
the configured stop threshold. Claim-grade red-stop also requires
`distance_to_stop_line_m`, ego `s/l`, a light color timeline, and either an
Apollo stop decision artifact or control full-stop evidence.

## Obstacle GT Contract

Truth-input obstacle publication must also be claim bounded. The bridge writes
`artifacts/obstacle_gt_contract.jsonl` when front-obstacle evidence is
available, and `tools/analyze_obstacle_gt_contract.py --run-dir <run> --out <run>/analysis/obstacle_gt_contract`
converts that stream to
`analysis/obstacle_gt_contract/obstacle_gt_contract_report.json`. Standard
natural-driving postprocess also writes this report; if the source stream is
missing it records `insufficient_data` rather than treating obstacle GT as
claim-grade. The report checks that the ego actor is not
published as an obstacle, CARLA actor ids map stably to Apollo perception ids,
position/theta/velocity frame evidence is declared, length/width/height are
positive, tracking time is monotonic, and dynamic actors are not represented by
zero-filled velocity unless they are actually stationary.

Lane-keep routes may legitimately publish empty
`/apollo/perception/obstacles` messages when no non-ego actor is part of the
scenario. In that case the obstacle contract report should record
`status=pass_empty`, `message_count>0`, `object_count=0`, and
`empty_obstacle_messages_healthy=true`. This is healthy lane-keep no-object
transport evidence, not proof that dynamic-obstacle behavior works. Follow-stop
or other dynamic-obstacle scenarios still require actual obstacle objects with
stable ids, frame/size/velocity evidence, and monotonic tracking time.

`Detection3DArray` or `MarkerArray` fallback sources often do not carry
velocity. Those artifacts are still useful recording evidence, but the report
marks them with `velocity_source_missing_or_zero_filled`; dynamic-obstacle
behavior claims such as follow-stop must not pass on that evidence alone.

## Apollo Channel Health

Natural driving cannot be judged only by whether the ego vehicle moves.
`apollo_channel_health_report.json` checks message count, frequency,
`max_gap_ms`, timestamp monotonicity, sequence monotonicity, and stale counts
for localization, chassis, obstacles, traffic light, planning, and control.

Traffic-light channel requirements are scenario-sensitive. Missing
`/apollo/perception/traffic_light` is a warning for lane-keep scenarios, but it
is a failure for `traffic_light_red_stop`, `traffic_light_green_go`, and
`traffic_light_red_to_green_release`.

The checker consumes exported `channel_stats.json` only. It does not start
CyberRT, parse binary records, modify the bridge, or prove behavior semantics:

```bash
python tools/analyze_apollo_channel_health.py \
  --config configs/algorithms/apollo_natural_driving_channels.yaml \
  --stats tests/fixtures/apollo/channel_stats_natural_valid.json \
  --scenario-class traffic_light_red_stop \
  --out /tmp/apollo_channel_health
```

## Gate Layers

### `assist_ledger`

Natural-driving reports read `assist_ledger.v1` evidence from the run
manifest, summary, config, bridge stats, or an explicit `assist_ledger.json`
when available.

Recorded assists include:

- `carla_direct_transport`
- `straight_lane_lateral_stabilizer`
- `straight_acc_override`
- `terminal_stop_hold`
- `goal_planner_module_disabled`
- `planning_common_dynamics_override`
- `speed_feedback_bridge_profile`
- `dummy_lateral`
- `legacy_followstop`
- `route_follower`
- `direct_autopilot`
- `manual_intervention`
- `force_green`

`carla_direct_transport` is recorded as a non-blocking transport candidate.
The other assists are blocking for unassisted natural-driving claims unless a
specific diagnostic experiment marks them `diagnostic_only`. Blocking assists
should produce a `warn`/diagnostic result such as
`assisted_pass_unassisted_claim_blocked`, not a clean unassisted
natural-driving pass. `natural_driving_report.json` must carry the
`assisted_pass` verdict for runs that satisfy behavior metrics only with a
blocking assist active.

Legacy harness fields require one extra check. A summary field such as
`lateral_mode=dummy` is a blocking `dummy_lateral` assist only when that
controller was actually applied. In Apollo CyberRT truth-input runs where the
harness control path is disabled and CARLA actuation comes from
`/apollo/control`, the run must explicitly record
`control_source=external_stack`, `harness_control_disabled=true`,
`legacy_controller_applied=false`, and an explicit clean `assist_ledger`.
Without those fields, the evaluator keeps the run non-claim-grade rather than
guessing that the dummy controller was harmless.

### Unassisted Apollo control claim gate

`can_claim_unassisted_natural_driving=true` is stricter than a scenario
`verdict=pass`. It requires all of the following evidence in
`natural_driving_report.json`:

- `backend=apollo_cyberrt`.
- `control_source=/apollo/control`.
- `routing_success_count >= 1`.
- `planning_nonempty_ratio >= 0.8`.
- `control_rx_count`, `control_tx_count`, and `control_apply_count` are all
  positive.
- `localization_contract_report.json` is `pass` or `warn`, claim-grade, and has
  no blocking reasons.
- `apollo_reference_line_contract_report.json` and
  `apollo_control_handoff_report.json` are `pass` or `warn` with no blocking
  stage/reason.
- `control_health_report.json` has no blocking failure.
- `assist_ledger.active_assists=[]` with explicit artifact evidence, or
  artifact-grade inference that no legacy controller/manual assist was applied;
  `assist_confidence=unknown` is insufficient for a hard claim.
- `lateral_guard_apply_count=0` and
  `trajectory_contract_guard_apply_count=0`; any replacement control remains a
  blocker unless explicitly diagnostic-only and not used for capability claim.
- Claim-grade traffic-light scenarios must use deterministic GT signal control,
  not `force_green`.

If any item is missing or violated, the evaluator records
`can_claim_unassisted_natural_driving=false` and adds the concrete blocker to
`why_not_claimable`. Smoke/debug scenarios with assists can remain useful as
`diagnostic_only`, but they are not unassisted Apollo natural-driving evidence.

### `link_health`

This gate proves the route is materially connected:

- `runtime_contract.status = aligned`
- `manifest.json` records reproducibility fields:
  `algorithm_variant_id`, `map`, `transport_mode`, `backend`, and an explicit
  truth-input marker such as `truth_input: true`;
- routing materialized
- planning non-empty
- Apollo control available
- required channels present at acceptable rates
- `apollo_channel_health_report.json` present for natural-driving claims
- `localization_contract_report.json` present and non-blocking for hard-gate
  behavior claims. A healthy `/apollo/localization/pose` channel is not enough
  by itself because pose reference point, frame transform, heading, timestamps,
  and velocity semantics must also be checked. Hard pass uses the report's
  `acceptance_checklist`: sim-time timestamps, configured CARLA-to-Apollo frame
  transform, runtime VRP/rear-axle conversion evidence, heading from transformed
  forward vector, quaternion/heading consistency, kinematics units, chassis
  speed consistency, and route/lane projection evidence must be `pass` or an
  explicitly allowed `warn`.
- `apollo_reference_line_contract_report.json` present and non-blocking for
  hard-gate behavior claims. This report verifies Apollo planning/control
  reference-line evidence; route-health heading agreement and bridge nearest
  lane projection diagnostics cannot satisfy this gate.

This gate does not prove behavior quality.

### `geometry_health`

This gate proves route geometry and ego relation are interpretable:

- `route_health.json`
- lateral error p95
- heading error p95
- `curve_segments.csv` when the scenario contains curve diagnostics
- `route_curve_artifact_gap_report.json` when the scenario contains curve
  diagnostics, so per-frame P1 matched/target/trajectory evidence is checked
  instead of inferred from summary text alone
- `localization_contract_report.json` with status `pass` or `warn` and no
  blocking reasons for lane-keep, junction, and traffic-light hard passes. If
  the report is missing, those scenarios stay `insufficient_data` rather than
  becoming Apollo algorithm failures.
- `apollo_reference_line_contract_report.json` with status `pass` or `warn`
  and no blocking reasons. If it is missing, `lane_keep`, `junction_turn`, and
  traffic-light hard gates stay `insufficient_data`; `curve_diagnostic` remains
  diagnostic-only rather than becoming an Apollo algorithm failure.
- for `curve_diagnostic`, route-health must expose matched-point anomaly,
  target-point anomaly, and Apollo raw-steer evidence. Missing fields keep the
  result `insufficient_data`; matched/target anomalies or first high steer keep
  the result from passing.

This is the first required evidence layer for curve lateral semantics claims;
the claim must reference `route_health.json` or
`apollo_lateral_semantics_report.json`.

Lane-keep and curve diagnostic scenarios have separate gate semantics:

- `lane_keep` requires `route_health.json`, `hard_gate_eligible=true`, required
  Apollo channel health for localization/chassis/planning/control, and
  control-health evidence. It also requires non-blocking
  `localization_contract_report.json`; summary success or vehicle motion alone
  cannot pass this gate.
- A route reconstructed from `timeseries.csv` remains diagnostic-only and
  produces `insufficient_data` for lane-keep hard gates.
- A blocking assist such as `straight_lane_lateral_stabilizer` can produce
  `assisted_pass`, but `can_claim_unassisted_natural_driving=false`.
- `curve_diagnostic` can consume diagnostic-only route evidence, but its
  verdict remains `diagnostic_only`; it is not a promotion hard gate.
- If `curve_diagnostic` lacks localization contract evidence, or the
  localization contract has blocking reasons such as non-monotonic timestamps,
  high lane-heading error, missing frame transform, or
  `position_uses_vrp=false`, the result must remain `diagnostic_only` or
  `insufficient_data`. This avoids blaming Apollo lateral logic before the GT
  localization substitute is proven semantically usable.
- Missing Apollo matched-point or target-point fields keep curve diagnosis at
  `diagnostic_only` or `insufficient_data`. They must not be turned into an
  Apollo algorithm failure claim.
- When `apollo_lateral_semantics_report.json` exists, the evaluator echoes
  `suspected_layer` and `confidence` so the report can point toward
  reference-line, target-point, control-mapping, or vehicle-response evidence
  without claiming an absolute root cause.

### `behavior_health`

This gate proves scenario behavior is plausible:

- lane keep routes progress without stuck/off-route behavior;
- junction routes traverse the junction without control handoff loss;
- red-light scenarios stop before the line;
- green-light scenarios proceed without unnecessary stop;
- red-to-green scenarios stop then release after green.

Behavior claims require route-health and scenario-specific artifacts. Demo
videos alone are not promotion evidence.

Traffic-light behavior claims additionally require non-blocking localization
contract evidence because stop-line distance and release timing depend on the
ego pose reference point and map-frame transform.

### `control_health`

This gate proves control is observable and not obviously contradictory:

- `control_health_report.json` generated from `summary.json` and
  `timeseries.csv/jsonl`;
- `control_handoff_status = control_consuming_with_nonzero_planning`
- raw/mapped/applied throttle, brake, and steer are present. If P0
  `timeseries.csv` lacks raw/mapped external-stack fields, analyzers may use
  `control_decode_debug.jsonl` or `bridge_control_decode.jsonl` for the
  Apollo raw and bridge mapped layers, but CARLA applied control and vehicle
  response still require applied/runtime artifacts;
- brake/throttle conflicts are bounded;
- mapped vs applied throttle/brake/steer mismatch is bounded;
- control latency is present, with high latency treated as warning evidence;
- control-actuation report is attached when mapping, `steer_scale`, or physical
  mapping claims are made.

When oscillation appears, `control_health_report.json` must decompose it before
any tuning claim:

- Apollo raw command oscillation;
- bridge mapped command oscillation;
- CARLA applied command oscillation;
- vehicle response oscillation;
- bridge apply cadence / same-frame drop / sync-tick issues.

Low bridge apply cadence or high same-frame drop should be treated as an apply
scheduling problem first. Do not smooth, clamp, or tune controls to hide a
localization or reference-line contract failure. Legacy mapping remains
smoke/debug evidence only; claim-grade natural-driving control mapping requires
physical/calibrated mapping or an explicit vehicle calibration profile.

For attribution, add:

- `control_attribution_report.json`;
- `control_attribution_summary.md`.

The attribution report separates source control semantics, bridge mapping,
CARLA apply, and vehicle yaw-rate response. It is specifically meant to prevent
ambiguous claims such as “Apollo is bad” or “the bridge is bad” without checking
raw, mapped, applied, and response evidence. If it reports
`source_control_semantics`, that still does not prove Apollo algorithm
limitation; route/reference-line/matched-target semantics must be checked.

For lateral semantics, add:

- `apollo_lateral_semantics_report.json`;
- `apollo_lateral_semantics_summary.md`.

This report correlates route/reference curvature, planning first kappa,
target-point kappa, source steer, matched/target anomalies, mapped/applied
steer, and yaw-rate response. It is the preferred artifact when a run shows
first-high-steer, matched-point-too-large, target-point jumps, or high kappa on
a straight reference. Its output is a suspected layer with confidence; it is
not a license to change `steer_scale` or declare Apollo algorithm limitation.

Calibration belongs here. It explains control-actuation behavior; it does not
replace route-health or shadow-mode evidence.

## Relationship To Reproduction Gate

Before turning a Town01 result into an Apollo capability claim, use
`docs/apollo_reproduction.md` and the reproduction gate:

- L0-L3 failures point to environment, replay, or adapter contract issues.
- L4 shadow-mode failure points to input contract or planning semantics before
  actuation.
- L5 closed-loop failure can be interpreted only after the earlier gates and
  relevant route-health/A-B/calibration artifacts are present.

## Apollo Link-Health Aggregator

For each online run, generate a quick blocker index after the individual
reports are available:

```bash
python tools/analyze_apollo_link_health.py \
  --run-dir runs/<run_id>
```

Outputs:

- `analysis/apollo_link_health/apollo_link_health_report.json`
- `analysis/apollo_link_health/apollo_link_health_summary.md`

The report summarizes environment/world, bridge runtime, channel health, GT
localization contract, HDMap projection, Apollo reference line, route
establishment, routing / planning / control handoff, control mapping/apply,
obstacle GT, prediction evidence, traffic-light GT, no-assist claim boundary, and final
natural-driving outcome. It is an evidence index only. Missing required
artifacts are `insufficient_data`, not pass. If planning only emits sparse
non-empty trajectories after routing, `route_establishment` should be the
primary blocker even when `/apollo/control` rx/tx counts are high. If
localization/reference-line lane-heading evidence is blocking, control
oscillation should stay a secondary blocker until those upstream contracts reach
`pass` or non-blocking `warn`.

## No-interference Apollo natural-driving claim checklist

This checklist is the operator workflow for a no-interference Apollo
truth-input claim. It is stricter than a short online smoke run. A
natural-driving hard pass must be represented by `natural_driving_report.json`
in the same suite or run packet; verbal observation, Dreamview, CARLA video, or
vehicle motion cannot replace that artifact.

Required online command for a claim-candidate suite:

```bash
/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/run_town01_natural_driving_suite.py \
  --suite configs/scenarios/town01_natural_driving_suite.yaml \
  --out runs/natural/<batch_id> \
  --classes lane_keep,curve_diagnostic,junction_turn,traffic_light_red_stop,traffic_light_green_go,traffic_light_red_to_green_release \
  --continue-on-failure \
  --postprocess-after-run \
  --fail-on-postprocess-status fail,warn,insufficient_data
```

Required run artifacts:

- `manifest.json`, `config.resolved.yaml`, `summary.json`, `timeseries.csv` or
  `timeseries.jsonl`, and `events.jsonl`.
- `manifest.json` must show `typed_config_loaded=true` and
  `legacy_fallback_used=false` for claim-grade runs. If a claim profile falls
  back to the legacy runner, the run is diagnostic-only even if Apollo channels
  and CARLA actuation are active.
- `artifacts/cyber_bridge_stats.json`,
  `artifacts/bridge_health_summary.json`,
  `artifacts/bridge_transport_summary.json`, and
  `artifacts/planning_topic_debug_summary.json` when available.
- `artifacts/carla_tick_health_summary.json` and
  `artifacts/carla_tick_health.jsonl` for harness world-tick cadence. A
  `CARLA_WORLD_TICK_TIMEOUT` before routing is an environment/world blocker,
  not Apollo routing or planning behavior evidence. A high inter-tick wall
  interval with low `max_tick_wall_duration_s` indicates harness loop cadence
  or wall-time pause evidence, not a single CARLA `world.tick()` timeout.
  `frame_loop_timing` rows identify the slowest harness stage between ticks.
- `artifacts/topic_publish_stats.jsonl` for claim-grade channel evidence. This
  row-level artifact distinguishes delivery wall rate, header sim-time rate,
  fresh CARLA world-frame rate, and payload counts for localization, chassis,
  obstacles, and traffic lights.
- `analysis/route_health/route_health.json`.
- `analysis/apollo_channel_health/apollo_channel_health_report.json`.
- `analysis/localization_contract/localization_contract_report.json`.
- `analysis/chassis_gt_contract/chassis_gt_contract_report.json`. The report
  must be claim-grade for no-interference Apollo claims; a healthy chassis
  channel alone is not enough because chassis speed, mode, gear, error code,
  feedback, and timestamp alignment are Apollo input semantics.
- `analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json` when
  `artifacts/apollo_hdmap_projection.jsonl` is available; otherwise
  reference-line hard gates remain `insufficient_data`.
- `analysis/apollo_route_contract/apollo_route_contract_report.json`. The
  scenario route length/start/goal/lane identity must be compatible with Apollo
  Routing output. A long warmup/debug route or mismatched Apollo route response
  blocks route establishment and cannot be hidden by later control activity.
  The preferred route response input is
  `artifacts/routing_response_decoded.json` or
  `artifacts/routing_response_decoded.jsonl`, decoded directly from
  `/apollo/routing_response`. If this artifact is missing, older
  Planning-derived route summaries are diagnostic fallback only and cannot
  create claim-grade route identity pass by themselves.
  The report also records `configured_scenario_route`, `last_routing_request`,
  `last_routing_response`, and `latest_planning_active_route_segment`; all four
  must identify the same claim route before lane-keep, curve, junction, or
  traffic-light behavior is interpreted as Apollo natural driving.
- `analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json`.
  Fallback merges from planning/control/timeseries artifacts must be
  timestamp/as-of joins within the configured tolerance, not index joins.
- `analysis/planning_materialization/planning_materialization_report.json` for
  non-empty `ADCTrajectory` ratio, empty-reason histogram, and
  `route_establishment` status. Control rx/tx evidence does not replace this
  report. `natural_driving_report.json` should expose
  `planning_nonempty_ratio_source`, `planning_nonempty_ratio_overall`, and
  `planning_nonempty_ratio_filtered` so startup/warmup dilution is visible
  without hiding a failed planning-materialization or route-establishment
  contract.
- `analysis/apollo_module_consumption/apollo_module_consumption_report.json`
  for Planning/Control consumption evidence, input timeout patterns,
  reference-line provider failures, prediction-not-ready logs, and empty
  Planning attribution. Bridge-side publish evidence alone is insufficient.
- `analysis/prediction_evidence/prediction_evidence_report.json`. Prediction
  may be `native_observed`, explicitly bypassed for a permitted static
  diagnostic case, or `not_required_for_case`; missing or unknown prediction
  state cannot silently pass the chain. Static lane-keep bypass with GT
  obstacles is diagnostic-only for natural-driving claim boundaries unless a
  scenario-specific override explicitly downgrades the claim.
- `analysis/apollo_control_handoff/apollo_control_handoff_report.json`.
- `analysis/control_health/control_health_report.json`.
- `analysis/apollo_link_health/apollo_link_health_report.json`.
- `analysis/traffic_light_contract/traffic_light_contract_report.json` and
  `analysis/traffic_light/traffic_light_behavior_report.json` for
  traffic-light scenarios.
- `analysis/obstacle_gt_contract/obstacle_gt_contract_report.json` for
  obstacle or follow-stop behavior claims.
- `analysis/natural_driving/natural_driving_report.json`,
  `natural_driving_report.csv`, and `natural_driving_summary.md`.

Required analyzers for a single online run:

```bash
RUN=runs/<run_id>
python tools/export_apollo_hdmap_projection.py \
  --run-dir "$RUN" \
  --container apollo_neo_dev_10.0.0_pkg \
  --map-dir /apollo/modules/map/data/carla_town01 \
  --base-map-filename base_map.txt \
  --map-name Town01 \
  --analyze
python tools/analyze_apollo_hdmap_projection.py \
  --projection "$RUN/artifacts/apollo_hdmap_projection.jsonl" \
  --out "$RUN/analysis/apollo_hdmap_projection"
python tools/analyze_apollo_localization_contract.py --run-dir "$RUN"
python tools/analyze_apollo_route_contract.py --run-dir "$RUN" --out "$RUN/analysis/apollo_route_contract"
python tools/analyze_routing_response_decoded.py \
  --input "$RUN/artifacts/routing_response_decoded.json" \
  --out "$RUN/analysis/routing_response_decoded"
python tools/analyze_apollo_reference_line_contract.py --run-dir "$RUN"
python tools/analyze_apollo_planning_materialization.py --run-dir "$RUN"
python tools/analyze_apollo_module_consumption.py --run-dir "$RUN" --out "$RUN/analysis/apollo_module_consumption"
python tools/analyze_apollo_prediction_evidence.py --run-dir "$RUN" --out "$RUN/analysis/prediction_evidence"
python tools/analyze_apollo_control_handoff.py --run-dir "$RUN"
python tools/analyze_apollo_link_health.py --run-dir "$RUN"
```

Required analyzer for the suite aggregate:

```bash
python tools/analyze_town01_natural_driving.py \
  --suite-root runs/natural/<batch_id> \
  --out runs/natural/<batch_id>/analysis
```

Strict postprocess for evidence gating:

```bash
python tools/postprocess_town01_natural_driving.py \
  --suite-root runs/natural/<batch_id> \
  --out runs/natural/<batch_id>/analysis/natural_driving \
  --require-full-target-coverage \
  --fail-on-status fail,warn,insufficient_data
```

Minimum pass thresholds for a claim-grade packet:

- `python -m pytest -q` passes before the online run is interpreted.
- `natural_driving_report.json.status=pass` and
  `capability_coverage.can_claim_full_natural_driving=true`.
- Every hard-gate scenario has `can_claim_unassisted_natural_driving=true` and
  an empty `why_not_claimable`.
- `manifest.json` includes a valid `algorithm_variant_id` and
  `algorithm_variant_manifest_path`. Missing, unresolved, mismatched, or
  non-`truth_input_closed_loop` variant metadata appears in
  `why_not_claimable` and blocks a capability claim.
- runtime claim boundary is `pass`: typed config loaded, no legacy fallback,
  and any config compatibility aliases are explicitly recorded. Claim review
  packets should include the materialized
  `analysis/runtime_claim_boundary/runtime_claim_boundary_report.json`; the
  evidence-bundle virtual summary is an index/fallback, not a substitute for a
  persisted claim-boundary artifact.
- A claim-profile typed run must dispatch through an explicit runtime boundary,
  not legacy fallback. Town01 online claim probes use
  `typed_apollo_claim_runtime`, which consumes the typed-resolved config and
  invokes the existing transition backend. CI/minimal configs without a
  runnable driver may use artifact-only `compat_apollo_cyber_gt_runtime`. In
  both cases, missing `/apollo/routing_response`, Apollo HDMap projection,
  localization, planning, or control evidence remains `insufficient_data`.
  Runtime dispatch is necessary evidence, but not a natural-driving pass.
- `localization_contract_report.json` is `pass`, claim-grade, uses sim-time,
  writes `header.frame_id=map`, uses verified VRP /
  rear-axle evidence, skips stale GT sample republish for claim-grade runs, and
  has no blocking reasons.
- `chassis_gt_contract_report.json` and
  `apollo_hdmap_projection_report.json` are both `pass` and claim-grade.
- `apollo_reference_line_contract_report.json`,
  `apollo_route_contract_report.json`,
  `planning_materialization_report.json`,
  `apollo_module_consumption_report.json`,
  `prediction_evidence_report.json`,
  `apollo_control_handoff_report.json`, `apollo_channel_health_report.json`,
  and `control_health_report.json` are `pass`. `warn` remains useful
  diagnostic evidence but does not satisfy the no-interference claim gate.
- `planning_materialization_report.json` reports valid same-domain timing:
  routing/planning latency cannot mix sim-time with wall-time, and input
  freshness cannot be claimed when topic as-of join coverage is zero.
- Apollo HDMap projection claim-grade evidence uses official
  `source=apollo_hdmap_api`, `ok_ratio >= 0.95`, heading `p95 < 0.05 rad`,
  lateral error `p95 < 0.50 m`, and lane-id compatibility with Planning
  `lane_id` or `target_lane_id` evidence.
- `apollo_link_health_report.json` reports
  `can_claim_unassisted_natural_driving=true`, no primary blocker, and no
  missing hard-gate artifacts.
- `assist_ledger.active_assists=[]`,
  `lateral_guard_apply_count=0`, and
  `trajectory_contract_guard_apply_count=0` or explicitly diagnostic
  non-replacement behavior.
- Explicit non-Apollo `control_source`, `controller`, or `lateral_mode`
  declarations override bridge `/apollo/control` rx counters. If they conflict
  with Apollo control-topic evidence, the run is `control_source_conflict`, not
  no-interference Apollo control.
- `control_apply_trace.jsonl` contains real command payloads. All-null
  raw/mapped/applied trace rows are no-command placeholders and cannot clear the
  control evidence gate.
- Traffic-light claim-grade scenarios use `traffic_light_policy=carla_actual`,
  not `force_green`, and include mapped signal/stop-line evidence plus behavior
  evidence.

Known non-claim-grade modes:

- `force_green`;
- `dummy_lateral`;
- `legacy_followstop`;
- `route_follower`;
- `assumed` vehicle reference;
- diagnostic nearest-lane projection only;
- stale localization republish used as fresh-sample evidence;
- missing HDMap / Apollo reference-line evidence;
- missing decoded `/apollo/routing_response` evidence;
- claim profile typed-config load failure or legacy fallback;
- missing, unresolved, or mismatched algorithm variant manifest;
- all-null `control_apply_trace.jsonl` rows used as control evidence;
- missing or unknown prediction evidence when the scenario requires prediction;
- explicit non-Apollo control source conflicting with `/apollo/control` rx/tx.

Recommended validation sequence:

1. Offline: `python -m pytest -q`.
2. Single-run postprocess: run the localization, reference-line,
   control-handoff, and link-health analyzers listed above, then run
   `python tools/analyze_town01_natural_driving.py --suite-root <suite> --out <suite>/analysis`.
3. Strict postprocess: run
   `tools/postprocess_town01_natural_driving.py` with
   `--require-full-target-coverage --fail-on-status fail,warn,insufficient_data`.
4. Online smoke: run the existing Town01 `lane_keep_097` path first for a short
   local smoke. Do not use it as a natural-driving claim. The target is
   `localization_contract` `pass`/`warn`,
   `apollo_reference_line_contract` at least `warn`, and
   `apollo_control_handoff` `pass`/`warn`.
5. Online claim candidate: run lane-keep, curve, junction, and traffic-light
   scenarios with `traffic_light_policy=carla_actual`. Only if every hard gate
   passes, every required artifact exists, and the assist ledger is clean may
   the suite set `can_claim_unassisted_natural_driving=true`.

Each final submission or handoff for this workflow should state the modified
files, newly generated artifact/report paths, pytest commands that passed,
what is still not proven, and the next online run or postprocess check that
should be performed. Keep this close-out factual; do not convert a smoke run
or diagnostic report into a natural-driving capability claim.

## CI-Friendly Validation

The suite schema is validated without CARLA, Apollo, ROS2, or CyberRT:

```bash
python -m pytest tests/test_natural_driving_suite_schema.py -q
```

The test ensures the suite loads, includes `097 / 217 / 031`, includes the
three traffic-light scenario classes, and does not claim full Apollo
perception/localization reproduction.

## Offline Postprocess Entry

After an online run or suite finishes, use the offline postprocess wrapper to
turn raw run directories into the standard evidence bundle:

```bash
python tools/postprocess_town01_natural_driving.py \
  --suite-root runs/natural/<batch_id> \
  --out runs/natural/<batch_id>/analysis/natural_driving
```

For automation or nightly checks, use strict status gating:

```bash
python tools/postprocess_town01_natural_driving.py \
  --suite-root runs/natural/<batch_id> \
  --out runs/natural/<batch_id>/analysis/natural_driving \
  --require-full-target-coverage \
  --fail-on-status fail,warn,insufficient_data
```

The strict form returns a non-zero exit code when the aggregate
`natural_driving_report.json` status is listed in `--fail-on-status`, or when
the suite has not proven every target class under
`capability_coverage.can_claim_full_natural_driving`. This is intended for
evidence gating only; it still does not prove full Apollo
perception/localization.

The postprocess CLI also prints the first actionable problem runs in JSON:
`problem_run_count`, `problem_runs`, `failed_runs`, `warning_runs`,
`insufficient_data_runs`, and `artifact_incomplete_runs`. Use those fields as
the first triage index after an online suite run before opening the full
`natural_driving_report.json`.

The lower-level analyzer supports the same strict status concept when you only
need to aggregate existing artifacts:

```bash
python tools/analyze_town01_natural_driving.py \
  --suite-root runs/natural/<batch_id> \
  --out runs/natural/<batch_id>/analysis \
  --require-full-target-coverage \
  --fail-on-status fail,warn,insufficient_data
```

The wrapper is intentionally offline-only. It does not start CARLA, Apollo,
ROS2, or CyberRT. It attempts to:

- generate `analysis/route_health/route_health.json` and companion route-health
  files when route/timeseries inputs are present;
- generate `analysis/route_curve_artifact_gap/route_curve_artifact_gap_report.json`
  from `timeseries.csv` and optional `summary.json`;
- generate `analysis/apollo_channel_health/apollo_channel_health_report.json`
  from `channel_stats.json`;
- if `channel_stats.json` is absent but `artifacts/cyber_bridge_stats.json`
  exists, derive a conservative `channel_stats.json` from bridge counters and
  mark the resulting channel-health report as estimated/warn-level evidence;
- generate `analysis/localization_contract/localization_contract_report.json`
  before the Apollo reference-line contract, so reference-line attribution can
  see whether GT localization is already blocking;
- generate `analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json`
  from planning/control reference-line artifacts and the localization contract
  when available;
- generate `analysis/traffic_light_contract/traffic_light_contract_report.json` from
  the configured Town01 Apollo route/signal contract and CARLA-to-Apollo
  traffic-light mapping; placeholder ids remain `warn`-level evidence;
- create an explicit `insufficient_data` traffic-light contract report when the
  contract or mapping config is unavailable;
- generate `analysis/traffic_light/traffic_light_behavior_report.json` for
  traffic-light scenarios and keep missing behavior metrics as
  `insufficient_data`;
- write `analysis/artifact_completeness/artifact_completeness_report.json` and
  a Markdown summary for every run so missing manifest fields, missing files,
  and missing P0 control trace fields are visible without opening the suite
  aggregate report;
- run the natural-driving evaluator and write
  `natural_driving_report.json`, `natural_driving_report.csv`, and
  `natural_driving_summary.md`.

When both legacy root-level reports and regenerated `analysis/...` reports
exist, the evaluator prefers the standard `analysis/...` artifact. This avoids
using stale root reports after a refreshed postprocess pass.
Route-contract postprocess must compare scenario and Apollo Routing XY in the
Apollo map frame, not by directly comparing raw CARLA world coordinates. The
report preserves `scenario_*_xy_carla` and writes transformed
`scenario_*_xy_apollo`; if the frame transform is unavailable, XY agreement is
warning/insufficient evidence rather than a hard route mismatch. A short
startup/ego-seed route is recorded as `startup_route_contract` and remains
diagnostic-only. A hard natural-driving claim requires
`claim_route_contract.materialized=true`; otherwise
`why_not_claimable` must include `claim_route_not_materialized`.
When `--refresh` is used but raw regeneration inputs are absent, postprocess
standardizes existing pass-level reports instead of overwriting them with
weaker placeholder-derived evidence. This applies to route-health, Apollo
channel-health, localization contract, Apollo reference-line contract, and
traffic-light contract reports; copied reports remain artifact evidence, not a
substitute for missing raw logs or records. For localization specifically,
ordinary P0 ego timeseries is not enough to regenerate claim-grade localization
evidence; postprocess requires strong localization fields or bridge
localization stats before replacing an existing localization contract report.
If raw regeneration inputs are present, `--refresh` must prefer regeneration
over a stale existing report. For example, bridge/channel/control handoff raw
artifacts should be re-analyzed rather than treated as a cached pass. The
fallback path is only for preserving already valid evidence when the raw inputs
needed to rebuild the same report are absent.
If `run_matrix.csv` exists, postprocess also refreshes artifact path columns and
updates `suite_manifest.json` so `actual_run_dir`, route-health, channel-health,
and traffic-light report paths point at the postprocessed evidence.

The broader Town01 goal postprocess (`tools/postprocess_town01_goal.py`) treats
input A/B batches as read-only evidence sources. It writes derived route-health,
route-curve gap, A/B, calibration, and audit artifacts under `--out`; it must
not create new `analysis/...` directories inside the input `runs/...` tree.

If required inputs such as `channel_stats.json`, route context, or
traffic-light contract/behavior evidence are missing, the generated reports
must stay `insufficient_data`. This is a diagnostic result, not a
natural-driving pass; `natural_driving_report.json` must preserve that boundary.
Bridge-counter-derived channel stats are useful for triage, but they are not
promotion-grade replacements for exported CyberRT monitor/record statistics
because message gaps and timestamp monotonicity are inferred from aggregate
counters.

The suite runner also exposes the same postprocess path:

```bash
python tools/run_town01_natural_driving_suite.py \
  --suite configs/scenarios/town01_natural_driving_suite.yaml \
  --out runs/natural/<batch_id> \
  --postprocess-existing
```

For staged local validation, generate the broader Town01 goal packet. The
packet includes A/B stages, demo recording, two natural-driving stages, and a
final audit stage that requires `natural_driving_report.json`:
`09_natural_driving_suite.sh`, `10_natural_driving_postprocess.sh`, and
`11_final_goal_audit.sh`.

```bash
/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/prepare_town01_goal_validation.py \
  --out /tmp/town01_goal_validation \
  --run-root runs/ab \
  --batch-prefix town01_goal_validation

bash /tmp/town01_goal_validation/09_natural_driving_suite.sh
bash /tmp/town01_goal_validation/10_natural_driving_postprocess.sh
bash /tmp/town01_goal_validation/11_final_goal_audit.sh
```

The natural-driving postprocess stage writes and gates
`natural_driving_report.json`. The final goal-audit stage passes both the
postprocess-local `calibration_report.json` and the natural-driving report into
`audit_town01_goal.py --fail-on-status incomplete`, so it returns non-zero
unless the full Town01 evidence chain is complete. A passing packet status is
evidence organization only; the natural-driving claim still requires the
report and run artifacts to be present and locally verified.
When the final audit is incomplete, `00_status.sh` / packet inspection surfaces
the final audit's `missing_evidence`, `next_actions`, and
`next_action_commands` directly from `town01_goal_audit.json`.

## Online Readiness Check

Before launching an online suite, run the readiness checker. It does not start
CARLA, Apollo, CyberRT, ROS2, or Dreamview; it only verifies that the suite can
be planned and that local runtime prerequisites are visible:

```bash
/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/check_town01_natural_driving_readiness.py \
  --suite configs/scenarios/town01_natural_driving_suite.yaml \
  --out /tmp/town01_natural_readiness \
  --strict-runtime
```

The checker writes:

- `town01_natural_driving_readiness.json`
- `town01_natural_driving_readiness.md`
- `run_matrix.preview.csv`
- `suite_manifest.preview.json`

The readiness report also includes `runtime.apollo_core_dumps`. Apollo
`application-core/data/core/core_*` files are process crash dumps, not driving
evidence. A large backlog can consume hundreds of GB and destabilize long
online suites, so strict readiness treats core-dump volume above the configured
threshold as a launch blocker. The checker only reports this condition; archive
or delete core dumps manually if they are no longer needed for crash debugging.

`status=ready` means the suite plan and local prerequisites are ready for a
local online attempt. `status=not_ready` should be treated as a launch blocker.
This readiness result is not behavior evidence: lane, curve, junction, and
traffic-light claims still require online run artifacts and
`natural_driving_report.json`.

For online execution, omit `--dry-run` only on a machine with the local
CARLA/Apollo stack already prepared:

```bash
/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/run_town01_natural_driving_suite.py \
  --suite configs/scenarios/town01_natural_driving_suite.yaml \
  --out runs/natural/<batch_id> \
  --classes lane_keep,junction_turn \
  --continue-on-failure \
  --postprocess-after-run \
  --fail-on-postprocess-status fail,warn,insufficient_data
```

The suite CLI defaults generated online child commands to
`/home/ubuntu/miniconda3/envs/carla16/bin/python3` when that interpreter exists.
Use `--python` only when deliberately validating another local runtime.

The non-dry-run form delegates to the existing online chain command for each
scenario and records stdout/stderr paths in `run_matrix.csv`. Runnable
lane/junction/curve scenarios are translated to the existing online-chain form:

```bash
/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/run_town01_capability_online_chain.py \
  --config configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml \
  --ticks <duration_s / fixed_delta_seconds> \
  --step <capability_profile>:<town01_rh_route_id>
```

Traffic-light scenarios now use existing signalized Town01 route probes such as
`town01_rh_spawn129_goal051` and `town01_rh_spawn219_goal063`, so they can enter
the online matrix. They remain `informational` because the tracked
CARLA-to-Apollo signal/stop-line mapping still contains placeholders and
historical runs are behavior-unhealthy. If a future scenario uses a placeholder
route ref, the runner marks it `runnable=false` /
`placeholder_route_ref_not_runnable` instead of executing an invalid online
command.

`suite_manifest.json` also records a `coverage` block. It summarizes runnable
and unrunnable rows by `scenario_class` and `gate_role`, including a
traffic-light subsection. This is planning/evidence coverage only. The
manifest also records `claim_boundary.can_claim_natural_driving_from_manifest =
false` because a Town01 natural-driving claim requires
`natural_driving_report.json` plus verified per-run artifacts, not just a
matrix or a dry-run command plan.

`natural_driving_report.json` records the stricter evidence gate in
`capability_coverage`. A subset report can still diagnose the runs it contains,
but it cannot support the full short-term objective unless
`capability_coverage.can_claim_full_natural_driving=true` and the suite verdict
is `pass`. A `warn` report can guide diagnosis, but it is not enough for a full
Town01 natural-driving claim. Planned suite roots with `run_matrix.csv` or
`suite_manifest.json` require coverage for `lane_keep`, `curve_diagnostic`,
`junction_turn`, `traffic_light_red_stop`, `traffic_light_green_go`, and
`traffic_light_red_to_green_release`; missing or unproven classes keep the suite
verdict at `insufficient_data`.

Non-dry-run execution is local verification only; CI should use `--dry-run` or
`--postprocess-existing` with fixtures.
When `--require-full-target-coverage` and
`--fail-on-postprocess-status fail,warn,insufficient_data` are set, the suite
runner returns a non-zero exit code if the generated natural-driving report is
not usable as complete evidence. This makes online regression failures,
missing scenario classes, and placeholder/warn-level evidence visible to shell
automation without changing the underlying CARLA/Apollo run commands.
The suite runner also mirrors the postprocess `problem_runs` payload when
`--postprocess-after-run` or `--postprocess-existing` is used, so online
automation can surface the first bad run without parsing the full report.

When a non-dry-run scenario finishes, `run_matrix.csv` keeps both layers of
paths:

- `run_dir`: the wrapper directory allocated by the natural-driving suite;
- `actual_run_dir`: the nested run directory discovered from the newest
  `summary.json` emitted by the existing Town01 online chain;
- artifact columns such as `summary_path`, `timeseries_path`,
  `route_health_path`, `apollo_channel_health_path`, and
  `traffic_light_contract_path`, and `traffic_light_behavior_path` are rewritten
  to the discovered run directory when those files exist.

This keeps the suite matrix reproducible while still respecting the current
online chain’s nested output layout.

## Route-Only Claim Probe And Planning Ratio Boundary

Use `configs/io/examples/town01_apollo_route_only_claim_probe.yaml` before a
lane-keep claim-candidate run when the route contract is the suspected primary
blocker. The profile inherits the current Town01 Apollo online profile, disables
startup short-route materialization and lane-follow fallback, and sends only the
scenario XY routing goal. It is a routing-contract probe, not a natural-driving
pass profile. A failed route-only probe means Apollo did not materialize the
configured scenario route, so downstream Planning/Control output remains
diagnostic.

For claim-grade route goals, the routing event must show that the goal
projection is `accepted=true`, `applied=true`, and sourced from a trusted lane
centerline. `accepted=false`, `source_trusted_lane_centerline=false`, goal snap
distance above `3m`, lateral error above `1m`, or available s-error above `5m`
blocks `apollo_route_contract_report.json` from passing. The legacy broad XY
threshold is retained only as a diagnostic fallback and cannot support a hard
gate.

`planning_nonempty_ratio_for_claim` is the claim-facing ratio. Filtered fields
such as `planning_nonempty_ratio_filtered_after_routing_segment_available` are
diagnostic context only; they may explain startup dilution, but they cannot
override a failing `planning_materialization_report.json` or a low overall /
claim-route Planning materialization ratio.

Claim-grade online profiles should keep artifact writing off the GT
localization/chassis hot path. The bridge now treats excessive artifact writer
queue depth as explicit backpressure evidence instead of letting diagnostic
JSONL writes silently slow `/apollo/localization/pose` or
`/apollo/canbus/chassis`. A run with artifact backpressure or large publish-gap
p95 values is diagnostic until channel health recovers.
