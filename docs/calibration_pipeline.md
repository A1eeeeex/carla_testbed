# Control-Actuation Calibration Pipeline

Calibration is the third Town01 mainline. It serves control-actuation evidence
only.

It is not the first curve fix, not sensor calibration, not an Apollo-wide
control parameter search, and not an automatic mechanism for editing Town01
mainline configs.

Calibration reports feed the Apollo reproduction gate in
`docs/apollo_reproduction.md`. They are optional for general route-health or
A/B interpretation, but required before any `steer_scale` change or physical
mapping promotion claim.

## Scope

The calibration profile and report pipeline cover:

- throttle response;
- brake response;
- steer response;
- control latency;
- whether the current `legacy` mapping and `steer_scale = 0.25` have supporting
  control-actuation evidence;
- whether physical mapping is worth a future gated re-test.

The pipeline does not cover:

- camera/lidar/radar calibration;
- perception calibration;
- Apollo full control tuning;
- automatic modification of `configs/io/examples/*` mainline configs.

## Profile

The tracked draft profile is:

```text
configs/calibration/control_actuation.yaml
```

Important defaults:

- `status: draft`
- `actuator_mapping_mode: legacy`
- `steer_scale: 0.25`
- physical mapping is not enabled by the profile
- `recommendation_policy.can_modify_mainline_config: false`
- `recommendation_policy.automatic_promotion: false`

The profile requires no-regression gates for:

- `097`
- `217`
- `031`

A `validated` profile requires all three gates to pass. Physical mapping also
requires these gates before it can be treated as a serious candidate.

## Trial Extraction

Real calibration evidence starts from run timeseries artifacts. The extractor
is offline and does not start CARLA, Apollo, ROS2, or CyberRT:

```bash
python tools/extract_calibration_trials.py \
  --run-dir runs/<run_id> \
  --out /tmp/calibration_trials.csv
```

It searches for `timeseries.csv`, `timeseries.jsonl`, or `timeseries.json` under
the run directory and cuts active control-command segments into the fixed
`calibration_trials.csv` schema.

Field preference is intentionally actuation-first:

- throttle: `throttle_applied`, then mapped/raw fallbacks
- brake: `brake_applied`, then mapped/raw fallbacks
- steer: `carla_steer_applied`, then mapped/raw fallbacks

Missing latency, yaw-rate, or route/backend fields do not fail extraction. The
corresponding output cells stay empty and the trial `notes` field records the
missing evidence. This keeps the extractor useful for old runs while preventing
it from inventing calibration certainty.

## Report

The offline report pipeline is:

```bash
python tools/extract_calibration_trials.py \
  --run-dir runs/<run_id> \
  --out /tmp/calibration_trials.csv

python tools/build_calibration_gate_results.py \
  --ab-report runs/ab/<hard_gate_batch>/analysis/ab_report.json \
  --out /tmp/calibration_gate_results.json

python tools/analyze_calibration_report.py \
  --profile configs/calibration/control_actuation.yaml \
  --trials /tmp/calibration_trials.csv \
  --gate-results /tmp/calibration_gate_results.json \
  --out /tmp/calibration_report_demo
```

Outputs:

- `calibration_report.json`
- `calibration_trials.csv`
- `calibration_plots/README.md`

Plots are not generated in the minimal pipeline. The directory is created so
future real-trial reports have a stable output location.

## `calibration_report.json`

Schema version: `calibration_report.v1`.

Top-level fields:

- `schema_version`
- `profile_id`
- `created_at`
- `inputs`
- `environment`
- `vehicle`
- `results`
- `no_regression`
- `recommendation`
- `missing_fields`
- `warnings`

`results` contains:

- `throttle_response`
- `brake_response`
- `steer_response`

`no_regression` contains gate status for `097`, `217`, and `031`.
`promotion_allowed` is true only when all required gates pass.
`tools/build_calibration_gate_results.py` converts the current A/B hard-gate
report into this gate input. It must not be replaced by hand-written pass
values unless the source evidence is explicitly documented.

`recommendation.enable_physical_mapping` remains false by default. A report may
explain evidence, but it must not update the mainline config.

## Graceful Degrade Rules

- Missing steer trials set `steer_response.supported=false` and add
  `steer_trials` to `missing_fields`.
- Missing latency fields keep latency summaries `null` and add warnings.
- Missing gate results set `promotion_allowed=false` and list all missing gates.
- Synthetic fixture success is not real calibration evidence.

## CI-Friendly Tests

These tests do not require CARLA, Apollo, ROS2, or CyberRT:

```bash
python -m pytest \
  tests/test_calibration_profile.py \
  tests/test_calibration_report.py \
  -q
```

## Local Validation

Real calibration still requires collecting CARLA trials. After collection, run
the same report command with real `calibration_trials.csv` and real gate
results. Any recommendation remains advisory until route-health no-regression
passes.

Core logic lives in `carla_testbed/calibration`. `tools/analyze_calibration_report.py`
is only a CLI wrapper.

For full Town01 evidence collection, `tools/postprocess_town01_goal.py` runs
the trial extraction, gate conversion, and calibration report generation as
part of the offline postprocess bundle. It still does not modify the mainline
configuration or promote physical mapping.
