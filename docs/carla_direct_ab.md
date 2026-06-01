# CARLA Direct A/B Workflow

This document describes how to compare `ros2_gt` and `carla_direct` without
over-reading transport results as behavior promotion.

## Current Status

`carla_direct` is an experimental Town01 candidate transport. It is not the
default backend.

The current short-window positive result supports a narrow claim: direct
transport/materialization can be promising on the compared routes. It does not
prove canonical curve tracking is healthy.

`ab_report.json` is an input to the Apollo reproduction gate described in
`docs/apollo_reproduction.md`. Without an A/B report, do not claim
`carla_direct` improvement. If reproduction L0-L4 are not pass/waived, do not
turn a closed-loop A/B result into an Apollo algorithm capability conclusion.

Recent `lane097` evidence shows why the candidate must stay gated. The
single-normalization canary `runs/ab/town01_single_norm_097_canary_20260523_163155`
proved the steering mode was `single_percent_at_select`, but direct control
writeback was too aggressive and produced a collision-heavy degradation. The
follow-up `runs/ab/town01_frame_flush_097_canary_20260523_165408` enabled
`algo.apollo.direct_bridge.control_apply_mode=frame_flush_only`; this removed
the direct collision failure, but `lane097` remained `candidate_degraded`
because lateral/heading error and lane invasions were still worse than
`ros2_gt`.

A follow-up offline/online comparison against the historical strong `lane097`
run showed the same steering normalization change also regressed `ros2_gt`:
historical `stitcher_v1` had `mapped_carla_steer_cmd` p95 around `7.9e-05` and
finished about `140m` with zero lane invasions, while the single-normalization
`ros2_gt` canary saturated steering at `0.25`, reached only about `6m`, and
reported lane invasions. For that reason, the current Town01 no-regression
configs pin `steering_percent_normalization=legacy_double_percent`. The
`single_percent_at_select` mode remains an explicit control-mapping candidate
that requires calibration evidence plus `097/217/031` no-regression before it
can be promoted.

The latest `legacy_double_percent` canary
`runs/ab/town01_legacy_norm_097_canary_fix_20260523_174455` proves the bridge
config propagation bug is fixed: both backends wrote
`steering_normalization_mode=legacy_double_percent` in
`bridge_control_decode.jsonl`. Current analyzer code treats very small
short-window route-completion deltas as tolerance-bounded noise, so this batch
reanalyzes as `lane097 candidate_positive` on the behavior metrics. It is still
not promotion evidence: the direct run reported a `mixed_observed` stale-frame
policy and a low direct-vs-baseline bridge cadence ratio, so the current
`always_republish` candidate still needs a fresh strict canary. Older direct
runs often lack an explicit stale-frame policy field; when the analyzer sees
both `direct_stale_world_frame_republish_count` and
`direct_stale_world_frame_skip_count`, it reports `mixed_observed`. Treat that
as legacy transport evidence, not as proof that the current
`always_republish` candidate has been tested.

`carla_direct` now exposes
`algo.apollo.direct_bridge.stale_world_frame_policy`:

- `until_control`: legacy behavior; republish cached GT only until control first
  appears.
- `always_republish`: experimental A/B candidate; keep harness as the only tick
  owner but republish the latest cached GT sample when CARLA has not advanced.
- `skip`: diagnostic mode; never republish stale frames.

The current direct candidate config uses `always_republish` for the next
`lane097` canary. This is a transport-cadence experiment, not a claim that
curve tracking is fixed.
The A/B runner also injects explicit runtime overrides into generated commands:

- both backends: `algo.apollo.control_mapping.steering_percent_normalization=legacy_double_percent`;
- `carla_direct`: `algo.apollo.direct_bridge.control_apply_mode=frame_flush_only`;
- `carla_direct`: `algo.apollo.direct_bridge.stale_world_frame_policy=always_republish`.

These overrides keep the canary honest if a tracked YAML default drifts. They
apply only to the generated A/B commands and do not change the mainline config.
Online matrix execution records each run's `return_code`, `actual_run_dir`,
`route_health_path`, `apollo_channel_health_path`, `control_health_path`, and
`route_health_error` in `ab_manifest.json` and carries them into `ab_report.*`.
If `--continue-on-failure` is not set, runs after the first failure are marked
`skipped` instead of remaining ambiguously `planned`.
The analyzer treats nonzero `return_code` and failed/timeout/skipped execution
status as blocking evidence: a run with residual artifacts but a failed process
cannot become `candidate_positive`, and strict inspection reports these fields
explicitly.
When `control_health_report.json` is available, the A/B report also carries
control apply delay, route progress after applied control, and ROS2 control
bridge frame-cadence/drop diagnostics. These fields help separate Apollo output
availability from control-bridge/apply-cadence problems; they are still
diagnostic evidence, not proof that `carla_direct` should become default.
When `apollo_channel_health_report.json` is available, the A/B report records
channel-health status and missing/low-rate channels so a transport candidate is
not judged only from vehicle motion or final route metrics.

## Canonical Route Assets

Tracked route-set configuration lives in:

```text
configs/routes/town01/canonical_five.yaml
```

Default hard gates:

- `lane097`: protects basic lane-keep behavior.
- `lane217`: protects lane/materialization/mild-curve behavior.
- `junction031`: protects junction/turn/routing-control handoff behavior.

Diagnostic curve gates:

- `curve217`
- `curve213`

The curve pair is required for diagnosis, but it is not a promotion hard gate
until route-health evidence shows the blocker has been fixed.

## A/B Manifest Discipline

Every interpreted A/B batch should have an `ab_manifest.v1` manifest.

The checker treats these fields as must-match between baseline and candidate:

- `route_id`
- `town_map`
- `route_definition_hash`
- `spawn_pose`
- `ego_blueprint`
- `vehicle_physics_hash`
- `fixed_delta_seconds`
- `sim_duration_s`
- `random_seed`
- `traffic_actors`
- `sensor_rig`
- `control_mode`
- `actuator_mapping_mode`
- `steer_scale`
- `guard_config_hash`
- `calibration_profile_id`
- `timeout_policy`

Allowed differences are transport-specific:

- backend
- transport mode
- bridge mode
- backend config path
- ROS2-GT vs CARLA-direct transport details
- direct control apply mode, when explicitly recorded as transport behavior
- direct stale-world-frame policy, when explicitly recorded as transport
  behavior

Missing route, map, spawn, fixed delta, duration, control mode, actuator mapping
mode, or `steer_scale` fails the manifest. Missing version/hash fields can
warn, but the warning must remain visible.

## Long-Window Matrix

The dry-run capable runner is:

```bash
python tools/run_town01_direct_ab.py \
  --route-config configs/routes/town01/canonical_five.yaml \
  --durations 30,60,120 \
  --baseline ros2_gt \
  --candidate carla_direct \
  --dry-run \
  --out /tmp/direct_ab_dryrun
```

Window intent:

- `30s`: routing/planning/control materialization and early behavior.
- `60s`: sustained route following and short-term route-health stability.
- `120s`: longer-window drift, fallback, stuck/off-route, and curve recovery.

By default the runner uses hard gates only. Add
`--include-diagnostic-curves` when the curve diagnostic pair should be included.

## Failure Reason Taxonomy

The A/B analyzer uses these failure categories:

- `artifact_missing`
- `planning_missing`
- `control_missing`
- `no_control`
- `stuck`
- `off_route`
- `high_lateral_error`
- `heading_divergence`
- `collision`
- `lane_invasion`
- `timeout`
- `bridge_drop`
- `insufficient_data`
- `success`

One failed run must not stop batch aggregation. It should appear as a failed or
insufficient run result while other route/backend pairs remain comparable.

## Multi-Metric Candidate Verdict

`candidate_positive` is not inferred from vehicle motion alone.

The analyzer requires:

- complete artifacts;
- successful execution status and zero or absent process return code;
- route completion evidence;
- planning/control/localization rate evidence;
- lateral error evidence;
- heading error evidence;
- a non-blocking failure reason.

Missing lateral or heading evidence makes the candidate insufficient for a
positive verdict, even if the vehicle moved.
Route-completion comparison has a small absolute tolerance for 30s windows so
one or two percentage points of rollout jitter is not mislabeled as a transport
regression. This does not relax lateral error, heading error, failure reason,
artifact completeness, or strict direct transport/cadence gates.

The analyzer writes:

- `ab_report.json`
- `ab_report.csv`
- `ab_report_summary.md`

Each interpreted run should also have:

- `summary.json`
- `manifest.json`
- `timeseries.csv` or `timeseries.jsonl`
- `analysis/route_health/route_health.json`
- `analysis/apollo_channel_health/apollo_channel_health_report.json`
- `analysis/control_health/control_health_report.json`

`ab_report.json.verdict.hard_gate_summary` is the first place to check after
any single-normalization, transport, or control-mapping change. It reports:

- `hard_gate_status`: `hard_gate_pass`, `hard_gate_fail`,
  `hard_gate_insufficient_data`, or `hard_gate_incomplete`;
- `positive_routes`: hard gates that passed the multi-metric comparison;
- `degraded_routes`: hard gates that regressed;
- `insufficient_routes`: hard gates with incomplete evidence;
- `missing_routes`: hard gates absent from the batch.

Only `hard_gate_pass` across `lane097`, `lane217`, and `junction031` allows the
change to proceed to curve promotion analysis. A curve `candidate_positive`
without hard-gate pass is diagnostic evidence only.

## CI-Friendly Commands

These do not start CARLA or Apollo:

```bash
python -m pytest \
  tests/test_canonical_routes.py \
  tests/test_ab_manifest.py \
  tests/test_direct_ab_dry_run.py \
  tests/test_ab_report.py \
  -q

python tools/run_town01_direct_ab.py \
  --route-config configs/routes/town01/canonical_five.yaml \
  --durations 30,60,120 \
  --baseline ros2_gt \
  --candidate carla_direct \
  --dry-run \
  --out /tmp/direct_ab_dryrun

python tools/analyze_ab_report.py \
  --batch-root tests/fixtures/ab/simple_batch \
  --out /tmp/ab_report_demo
```

## Local CARLA/Apollo Validation

Real A/B execution requires local CARLA/Apollo environment readiness. Use the
same canonical route set, fixed durations, and manifest consistency checker.
If a sample is blocked by startup/orchestration, classify it separately and do
not use it to overwrite existing strongest behavior evidence.

After a control-mapping or transport change, validate in this order:

1. Run `lane097` as the first canary.
2. If `lane097` does not regress, run `lane217` and `junction031`.
3. Re-run `tools/analyze_ab_report.py` and inspect
   `verdict.hard_gate_summary`.
4. Proceed to `curve217` / `curve213` only after hard gates are not degraded.

For steering mapping changes, also inspect `artifacts/bridge_control_decode.jsonl`
or `artifacts/control_decode_debug.jsonl`. The current Town01 no-regression
configs expect `steering_normalization_mode=legacy_double_percent`; do not
compare a batch that mixes this mode with `single_percent_at_select` unless the
batch is explicitly a steering-normalization experiment.
`tools/analyze_ab_report.py` reads this trace when present and marks a
baseline/candidate pair as `insufficient_data` if one side is missing the mode
or if the modes differ.
The markdown summary includes a `Steering Normalization Trace` table when trace
rows are present, so the operator can verify the mode without opening JSON.

Recommended command shape:

```bash
python tools/prepare_town01_direct_canary.py \
  --write-script /tmp/run_town01_direct_097_canary.sh

bash /tmp/run_town01_direct_097_canary.sh

python tools/inspect_town01_direct_canary.py --routes lane097
```

The preparation command does not start CARLA/Apollo. It writes a reviewed shell
script with the current strict transport contract flags and records the run root
in `/tmp/town01_direct_stale_republish_097_canary_root.txt` for later
inspection.
The inspection command reads that marker, regenerates `analysis/ab_report.*`,
and applies the same strict direct transport checks. It exits non-zero if the
direct candidate lacks observed contract alignment, cadence ratio evidence, or
the required steering normalization trace.

Equivalent expanded command:

```bash
RUN_ROOT="runs/ab/town01_control_mapping_gate_$(date +%Y%m%d_%H%M%S)"

/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/run_town01_direct_ab.py \
  --route-config configs/routes/town01/canonical_five.yaml \
  --durations 30 \
  --baseline ros2_gt \
  --candidate carla_direct \
  --routes 097,217,031 \
  --continue-on-failure \
  --carla-ignore-memory-preflight \
  --analyze-after-run \
  --require-hard-gate-pass \
  --require-steering-normalization-mode legacy_double_percent \
  --require-direct-control-apply-mode frame_flush_only \
  --require-direct-stale-world-frame-policy always_republish \
  --require-direct-transport-contract-aligned \
  --require-direct-bridge-cadence-ratio-min 0.8 \
  --out "$RUN_ROOT"
```

For a complete hard-gate decision, run `--routes 097,217,031` in one batch or
combine staged canaries manually. A batch that omits any of the three hard
gates will report `hard_gate_incomplete`; with `--require-hard-gate-pass`, the
analyzer returns a non-zero exit code until all three hard gates pass.
`tools/run_town01_direct_ab.py` also prints both `analysis_command` and
`strict_gate_command`, and stores them in `ab_manifest.json.analysis_commands`,
so operators do not need to reconstruct the analyzer command manually.
With `--analyze-after-run`, the runner itself writes `analysis/ab_report.*`; the
strict flags make the runner return non-zero when the gate fails.
The direct bridge cadence ratio gate compares `carla_direct` localization and
chassis publish Hz against the matched `ros2_gt` baseline for the same route and
duration. The default strict threshold is `0.8`; a lower ratio is treated as
transport evidence that is too weak for promotion, even if the ego vehicle moves.
If a report shows `direct_stale_world_frame_policy=mixed_observed`, rerun with
the current strict flags instead of using that run as `always_republish`
evidence.

## Offline Postprocess

After hard-gate, curve, and random batches are available, use the postprocess
wrapper to regenerate the stable evidence bundle without starting CARLA/Apollo:

```bash
python tools/postprocess_town01_goal.py \
  --hard-gate-batch runs/ab/<hard_gate_batch> \
  --curve-batch runs/ab/<curve_batch> \
  --random-batch runs/ab/<random_batch> \
  --demo-recording runs/<demo_run>/artifacts/town01_demo_recording_inspection.json \
  --out /tmp/town01_goal_postprocess
```

This writes:

- `ab/<label>/ab_report.json`
- route-health reports for runs that do not already have them
- `curve_pair/curve_pair_semantics.json` when both `curve217` and `curve213`
  route-health reports are present
- `calibration/calibration_trials.csv`
- `calibration/calibration_gate_results.json`
- `calibration/calibration_report.json`
- `audit/town01_goal_audit.json`
- `town01_postprocess.json`

The postprocess command is intentionally conservative. It does not make
`carla_direct` the default backend, does not infer curve health from short
transport evidence, and does not alter control mapping.

`tools/` remains a wrapper layer. Core route-set, manifest, dry-run matrix, and
analysis logic live in `carla_testbed/experiments` and `carla_testbed/analysis`.
