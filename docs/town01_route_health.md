# Town01 Route-Health Workflow

This document is the stable operator guide for the Town01 route-health analysis
surface. It explains what the offline analyzer can prove, what it can only
mark as missing, and where run artifacts should be written.

## Current Diagnosis Position

Town01 curve lateral semantics is the first mainline diagnosis target.

The current evidence says the remaining curve problem should be investigated
through route geometry, ego pose, Apollo lateral semantics, raw/mapped/applied
control, and guard involvement. Transport wins or demo videos are not enough to
promote curve behavior.

The route-health analyzer is intentionally tolerant of missing Apollo-specific
fields. Missing matched-point, target-point, or control fields should become
`missing_fields` entries, not uncaught exceptions.

Route-health reports are also an input to the Apollo reproduction gate
(`docs/apollo_reproduction.md`). Without a route-health report, do not claim a
curve lateral semantics conclusion, even if the closed-loop run moved the ego
vehicle.

## Core Modules

- `carla_testbed/routes/schema.py`
  - `RoutePoint`, `RouteDefinition`, `RouteHealthReport` dataclasses.
- `carla_testbed/routes/geometry.py`
  - route length, spacing, heading, curvature, nearest point, cross-track and
    heading error helpers.
- `carla_testbed/analysis/route_health.py`
  - offline route-health analysis.
- `carla_testbed/analysis/route_health_report.py`
  - report writer and run-directory integration.
- `carla_testbed/record/route_curve_fields.py`
  - P0/P1/P2 route-curve recorder field definitions.

`tools/analyze_route_health.py` is a wrapper. New core logic should go into the
package modules above, not into `tools/`.

## `route_health.json` Summary

`route_health.json` uses schema `route_health.v1` and has stable top-level
fields:

- `schema_version`
- `route_id`
- `map_name`
- `source`
- `route_geometry`
- `run_metrics`
- `apollo_semantics`
- `control_semantics`
- `missing_fields`
- `missing_inputs`
- `warnings`
- `verdict`

`route_geometry` includes point count, route length, spacing statistics,
heading jump statistics, curvature statistics, curve segments, spawn alignment,
and route direction consistency.

`run_metrics` can include lateral and heading error summaries when timeseries
data is available. It also summarizes P0 longitudinal/control-actuation
signals:

- `ego_speed_mean_mps`, `ego_speed_p95_mps`, `ego_speed_max_mps`
- `stopped_ratio`
- `ego_yaw_rate_abs_p95_rad_s`
- `throttle_raw_p95`, `throttle_mapped_p95`, `throttle_applied_p95`
- `brake_raw_p95`, `brake_mapped_p95`, `brake_applied_p95`
- `brake_throttle_conflict_frames`

These are diagnostic fields, not acceptance criteria by themselves. They help
separate "Apollo is planning/control is alive but the ego is not progressing"
from lateral-only curve tracking failures. If no timeseries exists,
`run_metrics` values stay `null` and `missing_inputs` includes `timeseries`.

## `curve_segments.csv`

`curve_segments.csv` has a fixed header:

```text
route_id,curve_segment_id,start_index,end_index,start_s,end_s,length_m,mean_abs_curvature,max_abs_curvature,direction,warning
```

Curve segments are consecutive route points whose absolute curvature exceeds
the configured threshold. The default static threshold is `0.03`.

## Recorder Field Tiers

### P0: Implemented

P0 fields are the minimum evidence needed to separate route geometry, ego pose,
raw Apollo control, bridge mapping, CARLA applied control, and guard flags.
They are defined in `ROUTE_CURVE_P0_FIELDS`.

Important P0 fields include:

- route: `route_id`, `route_index`, `nearest_route_index`, `route_s`,
  `route_x`, `route_y`, `route_z`, `route_heading`, `route_curvature`;
- ego: `ego_x`, `ego_y`, `ego_z`, `ego_heading`, `ego_speed`,
  `ego_yaw_rate`;
- errors: `cross_track_error`, `heading_error`, `curvature_at_nearest`,
  `curve_segment_id`, `is_curve_segment`, `is_junction_segment`;
- control: `apollo_steer_raw`, `bridge_steer_mapped`,
  `carla_steer_applied`, raw/mapped/applied throttle and brake;
- guard: `lateral_guard_applied`,
  `trajectory_contract_guard_applied`.

If a run lacks route, Apollo, or guard context, P0 fields should still appear
with `null` values. The field disappearing is a recorder problem.

### P1: Planned

P1 is for Apollo matched-point and target-point diagnosis:

- `apollo_matched_point_index`
- `apollo_matched_point_distance`
- `apollo_target_point_index`
- `apollo_target_point_distance`
- `apollo_trajectory_heading`
- `apollo_trajectory_curvature`

These fields are not required for the current CI workflow, but they are now
machine-checkable as an artifact gap. Use
`tools/check_route_curve_artifact_gap.py` on a run's `timeseries.csv` plus
optional `summary.json` to determine whether summary-level lateral semantics are
available without per-frame P1 evidence. That state is
`insufficient_data`, not curve-health proof.

Legacy Apollo debug columns such as
`apollo_debug_simple_lat_target_point_theta_rad` and
`apollo_debug_simple_lat_target_point_kappa` may be standardized into
`apollo_trajectory_heading` and `apollo_trajectory_curvature`. The standardizer
does not convert target-point `s` into `target_point_distance`; matched/target
distance still needs explicit per-frame evidence.

When control debug exposes target or matched point `x/y` and the row has ego
`x/y`, the standardizer may compute `apollo_target_point_distance` or
`apollo_matched_point_distance` directly from those coordinates. This is a
coordinate-distance diagnostic, not a substitute for Apollo module semantics.

### P2: Planned

P2 is for timing, trajectory, and calibration correlation:

- localization/chassis/planning/control timestamps;
- message ages and control latency;
- calibration profile id;
- actuator mapping mode;
- `steer_scale`.

P2 supports control-actuation interpretation. It is not the first curve fix.

## Lateral And Heading Error

The analyzer reads lateral error from `cross_track_error` first and falls back
to `lateral_error` if present. Heading error is read from `heading_error`.

If either field is missing, the report records the missing key in
`missing_fields`. It should still produce `route_health.json`,
`route_health.csv`, `curve_segments.csv`, and `route_health_summary.md`.

Missing Apollo matched-point or target-point fields must not fail route-health
analysis. They limit the claim, not the report generation.
When run-directory postprocess sees Town01 online summary fields such as
`first_high_steer_seq`, `first_matched_point_too_large_seq`, or
`apollo_simple_lat_target_point_kappa_abs_p95`, it copies them into
`route_health.json.apollo_semantics` as summary-derived evidence. This avoids
discarding curve semantic evidence that the online route-health runner already
computed, while still distinguishing it from per-frame P1 matched/target
timeseries.
At the natural-driving aggregation layer, however, a `curve_diagnostic` run
cannot pass without matched-point, target-point, and Apollo raw-steer evidence.
If those fields are missing, `natural_driving_report.json` must remain
`insufficient_data`; if route-health reports matched/target anomalies or
`first_high_steer`, the curve diagnostic run must fail rather than be counted
as healthy tracking.

## Run Artifact Path

Run-integrated route-health output belongs under:

```text
runs/<run>/analysis/route_health/
```

Expected files:

- `route_health.json`
- `route_health.csv`
- `curve_segments.csv`
- `route_health_summary.md`

`tools/analyze_route_health.py --run-dir <run>` writes this structure when a
route and/or timeseries can be discovered. If a route is missing, the verdict is
`insufficient_data` and `missing_inputs` includes `route`.

## CI-Friendly Tests

These tests do not require real CARLA, Apollo, ROS2, or CyberRT:

```bash
python -m pytest \
  tests/test_route_schema.py \
  tests/test_route_health.py \
  tests/test_route_health_report.py \
  tests/test_route_health_run_artifact.py \
  tests/test_route_curve_recorder_fields.py \
  -q
```

## Local Validation

Real route-health validation still needs local run artifacts from CARLA/Apollo.
After a real run, inspect:

```bash
python tools/analyze_route_health.py --run-dir runs/<run>
python -m carla_testbed inspect-run runs/<run>
```

Only aligned runtime runs with non-empty planning/control evidence should be
used for capability claims.

## Legacy Run Artifact Standardization

Older Town01 route-health runs may write useful evidence in legacy locations:

- `effective.yaml`
- `artifacts/scenario_metadata.json`
- `artifacts/debug_timeseries.csv`
- `artifacts/town01_route_health_state_timeline.jsonl`

The natural-driving postprocess now runs
`carla_testbed.analysis.run_artifact_standardizer.standardize_legacy_run_artifacts`
before generating route-health/control/channel reports. The standardizer can
derive:

- `manifest.json` reproducibility fields when they are present in the run
  config, summary, or scenario metadata;
- `route.json` from `scenario_metadata.route_trace`;
- `config.resolved.yaml` from `effective.yaml`;
- `events.jsonl` from the route-health state timeline;
- `timeseries.csv` from `debug_timeseries.csv`, with P0 control-trace aliases
  such as raw/mapped/applied steer, throttle, and brake.

This is an artifact-shape bridge only. It does not create behavior success,
does not invent route geometry when `route_trace` is missing, and leaves
unavailable fields empty so downstream reports remain `insufficient_data`
instead of being promoted incorrectly.
## Curve Pair Semantic Comparison

After `curve217` and `curve213` produce route-health reports, compare their
failure family with:

```bash
/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/analyze_curve_pair.py \
  --route-health <curve217>/analysis/route_health/route_health.json \
  --route-health <curve213>/analysis/route_health/route_health.json \
  --out <batch>/analysis/curve_pair_semantics
```

The comparator classifies:

- `shared_family_similar_onset`
- `shared_family_different_onset`
- `heterogeneous_blocker`
- `insufficient_data`

This is a route-health interpretation tool only. It must not be read as
`carla_direct` promotion evidence or as proof that the curve problem is solved.
