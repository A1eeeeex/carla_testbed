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

## Current Boundaries

- `carla_direct` remains an experimental candidate transport, not the default
  backend.
- `ros2_gt` remains the default baseline unless a specific run config says
  otherwise.
- `carla_direct` short-window positives do not prove curve lateral health.
- Calibration is control-actuation evidence only. It is not the first curve
  fix and must not automatically change `steer_scale` or promote physical
  mapping.
- Do not change Apollo parameters, enable physical mapping, or modify the core
  harness as part of this suite definition.

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
only truth-input stimulus evidence. A pass still requires Apollo HDMap
signal/stop-line contract evidence, `/apollo/perception/traffic_light` channel
health, `traffic_light_behavior_report.json`, and actual run metadata showing
`traffic_light_control` affected CARLA traffic-light actors. For red-to-green
release, that metadata must also contain a release event. Deterministic
stimulus alone does not prove traffic-light behavior is solved.
For red-stop scenarios, `red_stop_distance_m` alone is not enough for a pass:
the report also requires explicit `stopped_at_red=true` evidence, or derives it
from timeseries rows that show the ego close to the stop line with speed below
the configured stop threshold.

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
- for `curve_diagnostic`, route-health must expose matched-point anomaly,
  target-point anomaly, and Apollo raw-steer evidence. Missing fields keep the
  result `insufficient_data`; matched/target anomalies or first high steer keep
  the result from passing.

This is the first required evidence layer for curve lateral semantics claims.

### `behavior_health`

This gate proves scenario behavior is plausible:

- lane keep routes progress without stuck/off-route behavior;
- junction routes traverse the junction without control handoff loss;
- red-light scenarios stop before the line;
- green-light scenarios proceed without unnecessary stop;
- red-to-green scenarios stop then release after green.

Behavior claims require route-health and scenario-specific artifacts. Demo
videos alone are not promotion evidence.

### `control_health`

This gate proves control is observable and not obviously contradictory:

- `control_health_report.json` generated from `summary.json` and
  `timeseries.csv/jsonl`;
- `control_handoff_status = control_consuming_with_nonzero_planning`
- raw/mapped/applied throttle, brake, and steer are present;
- brake/throttle conflicts are bounded;
- mapped vs applied throttle/brake/steer mismatch is bounded;
- control latency is present, with high latency treated as warning evidence;
- control-actuation report is attached when mapping, `steer_scale`, or physical
  mapping claims are made.

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
- generate `analysis/traffic_light/traffic_light_contract_report.json` from
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
When `--refresh` is used but raw regeneration inputs are absent, postprocess
standardizes existing pass-level reports instead of overwriting them with
weaker placeholder-derived evidence. This applies to route-health, Apollo
channel-health, and traffic-light contract reports; copied reports remain
artifact evidence, not a substitute for missing raw logs or records.
If `run_matrix.csv` exists, postprocess also refreshes artifact path columns and
updates `suite_manifest.json` so `actual_run_dir`, route-health, channel-health,
and traffic-light report paths point at the postprocessed evidence.

If required inputs such as `channel_stats.json`, route context, or
traffic-light contract/behavior evidence are missing, the generated reports
must stay `insufficient_data`. This is a diagnostic result, not a
natural-driving pass.
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
