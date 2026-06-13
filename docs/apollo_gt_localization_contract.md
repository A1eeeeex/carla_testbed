# Apollo GT Localization Contract

This document defines the current truth-input localization boundary for Apollo
in CARLA. GT localization is not “missing localization.” It means CARLA ego
truth is converted into the semantics Apollo expects from
`LocalizationEstimate`.

This is not evidence that Apollo native localization has been reproduced.
Current truth-input runs replace the native Apollo localization module output
with a CARLA-derived adapter output. Any report or demo using this mode must say
so explicitly.

## Required Semantics

Apollo localization pose is interpreted in the Apollo map ENU frame. The
position reference point is the vehicle reference point, normally the rear axle
center. CARLA actor transforms are not automatically the same thing. A CARLA
actor origin can be at a blueprint-specific location, so the offset from CARLA
actor origin to Apollo VRP must be declared.

The repository now carries two CI-safe example contracts:

- `configs/town01/apollo_frame_transform.example.yaml`
- `configs/vehicles/ego_vehicle_reference.example.yaml`

Both examples are `confidence: assumed`; they are schema fixtures, not
claim-grade evidence.

## Frame Contract

CARLA uses Unreal/CARLA convention: X forward, Y right, Z up, left-handed.
Apollo map uses ENU: East, North, Up. A valid truth-input localization adapter
must explicitly declare the CARLA world to Apollo map transform:

- source frame and target frame;
- translation `tx`, `ty`, `tz`;
- `yaw_rad`;
- `scale`;
- whether the CARLA Y axis is flipped into Apollo ENU.

Do not silently treat CARLA world coordinates as Apollo map coordinates. The
frame transform must appear in a localization contract report before
curve/junction/traffic-light behavior is used for attribution.

## Vehicle Reference Point

Apollo position should be generated at the rear axle center / VRP. CARLA actor
origin must not be assumed to be the rear axle center. The vehicle reference
contract must declare:

- `vehicle_blueprint`;
- `apollo_reference_point`;
- `carla_actor_origin_definition`;
- `origin_to_vrp_carla` in CARLA actor coordinates.

`origin_to_vrp_carla` is interpreted in the CARLA actor local basis, not in the
global map frame: `x` is forward, `y` is right, and `z` is up. The adapter should
first move from actor origin to VRP in CARLA coordinates, then apply the CARLA
world to Apollo map frame transform. The CI-safe helper for this is
`carla_testbed.adapters.apollo.vehicle_reference.carla_vrp_to_apollo_position`.

If `carla_actor_origin_definition=unknown` or `confidence=assumed`, the
contract can support diagnostics only. It cannot support hard-gate natural
driving claims.

## Localization Dict Builder

`carla_testbed.adapters.apollo.localization_gt.build_localization_estimate_dict`
builds a protobuf-free, `LocalizationEstimate`-like dictionary from CARLA ego
truth. This is the contract object that a future CyberRT publisher can translate
to Apollo protobuf; it is not itself evidence that CyberRT publishing or Apollo
native localization has been reproduced.

The builder uses simulation time for `header.timestamp_sec` and
`measurement_time`, uses the VRP/rear-axle-center position from the vehicle
reference contract, computes heading from the transformed actor forward vector,
and converts linear velocity, optional acceleration, and optional angular
velocity into Apollo map semantics. If angular velocity arrives in degrees per
second, it must be converted to radians per second; if the unit is unknown, the
field is omitted and the output metadata records a warning.

The output metadata must retain `gt_localization=true`, `carla_frame_id`,
`position_reference`, map/frame transform identity, vehicle reference
confidence, angular velocity input unit, warnings, and missing fields. Missing
optional acceleration or angular velocity is acceptable for diagnostics, but it
must be visible in metadata rather than silently fabricated.

Apollo VRF fields use RFU axis order: `x=Right`, `y=Forward`, `z=Up`. They are
not FLU and not Forward/Right/Up. CARLA actor-local offsets remain
`x=forward`, `y=right`, `z=up`; the conversion into Apollo VRF must explicitly
swap into RFU semantics.

Apollo `pose.orientation` is the rotation from vehicle RFU coordinates to
Apollo ENU world coordinates. It is not an ordinary ENU yaw quaternion. For a
vehicle heading East (`heading=0`), the RFU-to-ENU quaternion is not identity:
the vehicle forward axis maps to ENU East and the vehicle right axis maps to
ENU South. The GT builder records `orientation_convention=RFU_to_ENU`,
`heading_from_forward_vector`, and `quaternion_heading_diff_rad` so this can be
audited later.

`carla_testbed.adapters.apollo.messages` is the shared dict-to-protobuf-like
write boundary for online bridge code. The Apollo Cyber bridge should not
inline a second copy of `LocalizationEstimate` semantics. It must write
`header.timestamp_sec`, `header.frame_id`, `measurement_time`, pose, RFU-to-ENU
orientation, kinematics, and debug metadata from the same canonical payload.

For the current ROS odometry bridge path, heading is sourced from odom
quaternion yaw after CARLA-to-Apollo frame transform, so debug metadata should
record `heading_source=odom_quaternion_yaw_after_frame_transform`. It must not
claim `transformed_forward_vector` unless the runtime actually used CARLA actor
forward/basis vectors. The bridge should still publish an RFU-to-ENU
orientation whose independently decoded forward heading equals `pose.heading`.

Claim-grade online localization evidence now requires:

- `LocalizationEstimate.header.frame_id` actually written as `map`;
- `LocalizationEstimate.header.timestamp_sec == measurement_time` within 1 ms;
- `orientation_convention=RFU_to_ENU`;
- RFU quaternion decoded heading p95 difference below `1e-3 rad`;
- declared velocity/acceleration/angular-velocity frames and units;
- `vehicle_reference_confidence=verified` and
  `vehicle_reference_hard_gate_eligible=true`;
- bridge claim-grade publish policy configured to skip stale GT samples instead
  of republishing cached localization/chassis samples as fresh evidence.

Stale republish rows may still be useful diagnostic evidence, but duplicate
localization timestamps block claim-grade natural-driving evidence unless the
run is explicitly marked as non-claim/debug.

## Localization Contract Report

`carla_testbed.analysis.localization_contract` reads run artifacts and writes
`localization_contract_report.json` plus `localization_contract_summary.md`. The
report combines `timeseries.csv/jsonl`, `channel_stats.json`,
`route_health.json`, the frame transform YAML, and the vehicle reference YAML to
check whether the truth-input localization evidence is good enough for later
curve, junction, or traffic-light attribution.

The report checks `/apollo/localization/pose` rate, message count, timestamp and
sequence monotonicity, timestamp deltas against chassis/planning evidence,
configured frame transform presence, VRP/rear-axle reference confidence, heading
consistency, velocity/chassis speed consistency, yaw-rate consistency, and
availability of status/uncertainty fields. Missing historical fields must become
`missing_fields` and `warnings`; they must not be fabricated.

Yaw-rate consistency can use an explicit `heading_fd_yaw_rate` field when one is
recorded, or the analyzer can compute a finite-difference yaw rate from
`localization_heading` and `localization_header_timestamp_sec`. This makes the
check independent of a bridge-provided self-report while still avoiding CARLA
runtime imports.

In truth-input mode, Apollo MSF status, native sensor status, and pose
uncertainty are not equivalent to native Apollo localization outputs. If these
are not modeled, the bridge should record explicit policy fields such as
`not_modelled_gt_truth` or `not_applicable_gt_truth`. The checklist then remains
`warn`, not `pass`, but the gap is no longer an unexplained missing field.

Channel health separates publish rate from fresh-sample rate. Claim-grade
online runs should set `bridge.claim_grade.enabled=true` and
`stale_world_frame_policy=skip`, so localization/chassis/obstacle messages are
published once per new CARLA world frame or odometry timestamp. The bridge
writes `artifacts/topic_publish_stats.jsonl` with `channel`, wall time,
sim/header timestamp, sequence, frame id, CARLA world frame, and payload counts.
The channel-health normalizer uses this row-level evidence to compute
`delivery_wall_hz`, `header_sim_hz`, and `fresh_world_frame_hz`.

Legacy or diagnostic bridge modes may still republish the latest localization
sample while waiting for a new CARLA/ROS2 odometry stamp. Therefore `loc_count`
or raw publish count is not automatically the number of fresh localization
samples. Duplicate timestamps are allowed as diagnostic evidence of republish
behavior, but duplicate rows must not be counted as fresh samples, and
claim-grade hard pass remains blocked when `duplicate_timestamp_ratio >= 0.01`
unless the run is explicitly marked as non-claim/debug.

The reference-point check distinguishes configured intent from runtime evidence.
`vehicle_reference.yaml` can say the configured Apollo reference point should be
rear axle center, but that alone does not prove the published localization
position used VRP. Claim-grade evidence needs runtime artifacts such as
`ros2_gt_live_stats.json` and `cyber_bridge_stats.json`. A valid current path is:
ROS2 GT publishes CARLA vehicle-origin odometry, then the Cyber bridge publishes
Apollo localization as rear-axle/VRP using an explicit
`localization_back_offset_m`. The report records this as
`source_vehicle_origin_to_published_rear_axle`. If both source and published
references remain `vehicle_origin`, `position_uses_vrp=false` blocks hard-gate
claims.

The frame transform report separates affine self-consistency from actual map
alignment. `affine_self_consistency_roundtrip_error_m_max=0` only means the
configured transform is internally invertible; it does not prove the Apollo
HDMap, lane projection, or reference line is aligned. Those claims require
separate route projection, reference-line, or HDMap contract artifacts.

## Apollo HDMap Projection Artifact

Claim-grade Apollo lane projection evidence can be supplied by
`artifacts/apollo_hdmap_projection.jsonl`. This file is optional because it
must be generated inside the Apollo environment or from an Apollo Cyber record
using the Apollo HDMap API; CI does not build or import that runtime.

Each JSONL row records a localization sample, nearest Apollo lane id,
projection `s/l`, lane heading at `s`, heading error, lateral error, map name,
map directory, and `status`. The row is claim-grade only when
`source="apollo_hdmap_api"`. Bridge-side nearest-lane diagnostics, CARLA
waypoint projection, route-health projection, or reconstructed route projection
must not be labeled as this source.

When the artifact is missing, analyzers report
`apollo_hdmap_projection_available=false` and the projection section remains
`insufficient_data`; they do not fabricate `reference_line_verified=true`. When
official projection rows exist but heading or lateral error is high, the
localization/reference-line reports fail and prioritize map alignment, lane
direction, lane id, or routing snap investigation before any Apollo behavior
claim.

Operators can inspect the artifact directly before running the broader
reference-line or natural-driving gates:

```bash
python tools/export_apollo_hdmap_projection.py \
  --run-dir "$RUN" \
  --container apollo_neo_dev_10.0.0_pkg \
  --map-dir /apollo/modules/map/data/carla_town01 \
  --base-map-filename base_map.txt \
  --map-name Town01 \
  --max-samples 250 \
  --analyze

python tools/analyze_apollo_hdmap_projection.py \
  --projection "$RUN/artifacts/apollo_hdmap_projection.jsonl" \
  --out "$RUN/analysis/apollo_hdmap_projection"
```

The exporter reads actual Apollo-map localization samples from
`artifacts/apollo_reference_line_contract.jsonl` and invokes Apollo's
`/opt/apollo/neo/bin/map_xysl` inside the package container. `map_xysl` calls
Apollo HDMap nearest-lane and lane-heading APIs, so rows emitted by this wrapper
may use `source="apollo_hdmap_api"`. If the wrapper cannot run, keep the
projection layer as `insufficient_data`; do not substitute CARLA waypoint or
bridge nearest-lane diagnostics for claim-grade projection evidence.
Use bounded sampling and a per-sample timeout for online runs. A timed-out
`map_xysl` call should become a row with `status="error"` /
`command_timeout=true`, not a skipped row and not a pass.

When `localization_contract_report.json` has both official HDMap projection
rows and CARLA route `cross_track_error`, it also reports
`hdmap_route_lateral_consistency`. If Apollo HDMap `projection_l` matches CARLA
route cross-track after the expected sign convention, the high lateral error is
more likely actual closed-loop lateral drift / route tracking evidence than a
static frame-transform or lane-namespace mismatch. This attribution does not
turn a high-lateral projection into a pass; it only narrows the next debugging
layer.
`natural_driving_report.json` should keep the run non-claimable and use
`failure_reason=actual_lateral_drift_matches_hdmap_projection` for this narrow
case, while `apollo_link_health_report.json` should surface the same condition
as `natural_driving_outcome:actual_lateral_drift_matches_hdmap_projection`.
This is an attribution refinement, not a pass condition: the localization and
HDMap projection layers remain blocking until lateral error is within gate
thresholds.

`analysis/apollo_hdmap_projection/apollo_hdmap_projection_report.json` is a
projection evidence report only. A `pass` result can support
`contracts.apollo_hdmap_projection`; it does not replace
`apollo_reference_line_contract_report.json`, `localization_contract_report.json`,
or `natural_driving_report.json`.

`manifest.scenario_metadata.route_trace` or
`manifest.metadata.scenario_metadata.route_trace` may provide claim-grade CARLA
route geometry for route-health. That is still not Apollo reference-line
verification. A route trace can make `route_health.json` eligible for CARLA
route-geometry gates while `reference_line_verified=false` and
`apollo_reference_line_claim_grade=false` remain true until an explicit
`reference_line_validation_report.json`, route projection report, or HDMap
contract report exists.

Natural-driving hard gates now separate localization semantics from Apollo
reference-line semantics. `localization_contract_report.json` checks time base,
frame transform, VRP/rear-axle reference, heading, quaternion, velocity,
yaw-rate, and route/lane diagnostic consistency. It must not treat a bridge
nearest-lane diagnostic as proof that Apollo Planning and Control are using a
compatible reference line. That claim is carried by
`apollo_reference_line_contract_report.json`, which now records three separate
subcontracts: `contracts.planning_trajectory` for non-empty trajectory and
trajectory heading semantics, `contracts.control_reference` for Control debug
reference heading/lateral semantics, and
`contracts.apollo_hdmap_projection` for official Apollo HDMap API lane
projection. A run may have usable planning trajectory evidence while remaining
`insufficient_data` for claim-grade reference-line verification if the control
reference or Apollo HDMap projection artifact is missing.
If the localization route-heading error is small but the Apollo reference-line
contract fails or is missing, `natural_driving_report.json` must remain
`insufficient_data` or diagnostic-only rather than claiming Apollo natural
driving.

Runtime rows or localization debug artifacts should expose:

- `localization_header_timestamp_sec`, `localization_measurement_time`, and
  `localization_sequence_num`;
- `localization_frame_id`, which should match the actual Apollo header
  `frame_id`;
- `localization_heading`, `localization_orientation_yaw`,
  `orientation_heading_diff_rad`, `heading_source`, and
  `orientation_convention`;
- `localization_speed_mps`, `chassis_speed_mps`,
  `velocity_norm_vs_chassis_speed_mps`, `ego_yaw_rate`,
  `heading_fd_yaw_rate`, and `yaw_rate_vs_heading_fd_rad_s`;
- `localization_angular_velocity_unit`, `linear_acceleration_available`,
  `linear_acceleration_vrf_available`, `angular_velocity_vrf_available`, and
  `acceleration_source`.

Orientation diagnostics should include `localization_orientation_qx`,
`localization_orientation_qy`, `localization_orientation_qz`, and
`localization_orientation_qw`. The analyzer independently decodes the
RFU-to-ENU quaternion by rotating the RFU forward axis and comparing the decoded
heading with `localization_heading`. If only a self-reported
`orientation_heading_diff_rad` exists, the report can be useful for diagnostics
but should not be treated as the same evidence level as an independent decode.

CARLA `Actor.get_angular_velocity()` returns degrees per second; ROS odometry
and Apollo localization diagnostics should use radians per second. CARLA
`ActorSnapshot.get_angular_velocity()` uses radians per second. Do not mix these
two sources without recording the source API and unit.

Acceleration fields distinguish presence from physical availability. The current
bridge should compute map-frame and VRF linear acceleration from finite
differences over simulation-time velocity samples after the first fresh sample.
If a stale localization sample is republished, the row should be marked
`acceleration_source=stale_timestamp_republish`; analyzer evidence should prefer
later `finite_difference` samples over first-row initialization. If
`acceleration_source=zero_filled`, then acceleration is present as a protobuf
field but is not physical acceleration evidence. Reports should set
`physical_linear_acceleration_available=false`,
`linear_acceleration_claim_grade=false`, and warn with
`acceleration_zero_filled`. Zero-filled acceleration alone should not block a
lane-keep diagnostic, but it must not be used as claim-grade dynamics evidence.

The report also contains `acceptance_checklist`, a machine-readable checklist
for the final localization acceptance questions:

- `localization_channel_health`: `/apollo/localization/pose` exists, rate is
  reasonable, timestamps and sequence numbers are monotonic, and stale messages
  are visible.
- `sim_time_time_base`: header timestamp and `measurement_time` use simulation
  time rather than an untracked wall-time source.
- `frame_transform_configured`: CARLA world to Apollo map transform is
  explicitly configured, including axis flip/mapping.
- `position_uses_vrp`: Apollo position uses rear axle center / VRP rather than
  raw CARLA actor origin.
- `source_to_published_reference_explained`: the source reference point and the
  published Apollo localization reference are explicitly explained.
- `heading_from_transformed_forward`: legacy checklist key retained for
  compatibility; the check now means the heading source is truthfully declared
  as either transformed actor forward vector or odom quaternion yaw after frame
  transform.
- `rfu_to_enu_orientation`: orientation declares the RFU-to-ENU Apollo
  convention.
- `decoded_orientation_consistency`: orientation quaternion and heading are
  consistent when independently decoded from qx/qy/qz/qw.
- `kinematics_frame_and_units`: velocity, acceleration, angular velocity, and
  VRF fields use declared frame/unit semantics.
- `chassis_speed_consistency`: localization speed and chassis speed agree
  within configured tolerance.
- `lane_projection`: ego pose has diagnostic lane projection evidence.
- `reference_line_projection`: Apollo reference-line projection is explicitly
  verified when a scenario requires it.
- `acceleration_semantics`: acceleration fields are not mistaken for
  claim-grade evidence when zero-filled.
- `uncertainty_status_policy`: uncertainty and status fields are available or
  explicitly treated as policy gaps.
- `natural_driving_gate_ready`: localization evidence does not block
  `natural_driving_report.json` hard-gate claims.

Each checklist item has `status`, `question`, `evidence_fields`, and `reason`.
If required evidence is absent, the item must be `insufficient_data`, `warn`, or
`fail`; it must not be silently treated as `pass`.

This report is a prerequisite localization/input-contract artifact. A passing
`localization_contract_report.json` does not by itself prove natural driving,
planning correctness, or Apollo native localization reproduction. It only
reduces the chance that a later `route_health.json` or
`apollo_lateral_semantics_report.json` conclusion is actually caused by bad GT
localization semantics.

This workflow does not enable Apollo native localization. It remains a
truth-input MVP where CARLA truth is adapted into Apollo localization semantics.

## Heading And Dynamics

Heading should not be copied directly from CARLA yaw. The adapter should
transform the actor forward vector into Apollo map ENU and compute heading from
that converted vector. This prevents left-handed/right-handed and axis-flip
mistakes from hiding inside one yaw scalar.

Velocity, acceleration, and angular velocity must be in Apollo map semantics.
Angular velocity units must be explicit, especially when data could be mixed
between degrees per second and radians per second. Any contract report should
state the chosen unit.

## Evidence Boundary

A localization contract report is prerequisite evidence before using
curve/junction/traffic-light failures as Apollo behavior conclusions alongside
`route_health.json` or `apollo_lateral_semantics_report.json`. Without the
frame transform and vehicle reference contracts, a curve tracking or junction
issue may still be a localization/input-contract issue rather than a planning
or control issue.

Related artifacts:

- `route_health.json`
- `apollo_lateral_semantics_report.json`
- `town01_apollo_contract_report.json`
- `localization_contract_report.json`

The schema validator is CI-safe and does not import CARLA, CyberRT, or Apollo
protobufs:

```bash
python -m pytest tests/test_apollo_gt_localization_contract_schema.py -q
python -m pytest tests/test_apollo_localization_contract.py -q
```

## Validation And Fix Loop

Start with offline self-checks:

```bash
python3 -m pytest \
  tests/test_apollo_gt_localization_contract_schema.py \
  tests/test_apollo_frame_transform.py \
  tests/test_apollo_vehicle_reference.py \
  tests/test_apollo_localization_gt.py \
  tests/test_apollo_localization_contract.py \
  tests/test_natural_driving_localization_gate.py -q
```

Then generate a synthetic report:

```bash
python tools/analyze_apollo_localization_contract.py \
  --timeseries tests/fixtures/localization_contract/complete_timeseries.csv \
  --channel-stats tests/fixtures/localization_contract/channel_stats.json \
  --route-health tests/fixtures/localization_contract/route_health.json \
  --frame-transform configs/town01/apollo_frame_transform.example.yaml \
  --vehicle-reference tests/fixtures/localization_contract/vehicle_reference_verified.yaml \
  --out /tmp/localization_contract_check
cat /tmp/localization_contract_check/localization_contract_summary.md
```

The tracked `configs/vehicles/ego_vehicle_reference.example.yaml` is
`confidence: assumed`, so it should produce a `warn` report rather than a
claim-grade `pass`. That warning is intentional until the rear-axle / VRP offset
is measured or otherwise verified for the online vehicle blueprint. The current
claim-grade runtime profile is
`configs/vehicles/ego_vehicle_reference.verified.yaml`, which records the
bridge-verified Lincoln MKZ rear-axle offset used by the GT localization path.

An online `warn` with no blocking reasons is useful diagnostic evidence, but it
is not automatically claim-grade. `natural_driving_report.json` must inspect the
`acceptance_checklist`; hard pass is blocked if simulation time, configured
frame transform, runtime VRP conversion, heading/quaternion consistency,
kinematics units, chassis speed consistency, or route/lane projection evidence
is missing or failing.

For an online run:

```bash
RUN=runs/<run_id>
python tools/analyze_route_health.py --run-dir "$RUN"
python tools/analyze_apollo_localization_contract.py \
  --run-dir "$RUN" \
  --frame-transform configs/town01/apollo_frame_transform.example.yaml \
  --vehicle-reference configs/vehicles/ego_vehicle_reference.verified.yaml \
  --out "$RUN/analysis/localization_contract"
cat "$RUN/analysis/localization_contract/localization_contract_summary.md"
cat "$RUN/analysis/route_health/route_health_summary.md"
```

Use this fix order:

- channel/timestamp failures: inspect CyberRT publisher, bridge time adapter,
  and sim-time source before rerunning the same route;
- frame-transform failures: verify CARLA world to Apollo map alignment before
  changing route or control parameters;
- VRP failures: measure actor origin to rear-axle offset and update the vehicle
  reference config; never assume actor origin is Apollo position;
- heading/quaternion failures: check that heading comes from the transformed
  forward vector, not raw CARLA yaw;
- velocity/yaw-rate failures: check frame conversion and deg/s versus rad/s;
- route/lane projection failures: check localization transform, VRP, route
  health, and Apollo map/reference-line contracts before blaming Apollo
  lateral behavior.

## Operator Claim Gate

For no-interference Apollo truth-input evidence, localization is a prerequisite
contract, not the final behavior verdict. A lane, curve, junction, or
traffic-light hard gate can only be interpreted through
`natural_driving_report.json` after the localization report is attached.
Use the full "No-interference Apollo natural-driving claim checklist" in
`docs/apollo_town01_truth_natural_driving.md` before turning this report into a
capability claim.

Operator sequence:

```bash
RUN=runs/<run_id>
python tools/analyze_apollo_hdmap_projection.py \
  --projection "$RUN/artifacts/apollo_hdmap_projection.jsonl" \
  --out "$RUN/analysis/apollo_hdmap_projection"
python tools/analyze_apollo_localization_contract.py --run-dir "$RUN"
python tools/analyze_apollo_reference_line_contract.py --run-dir "$RUN"
python tools/analyze_apollo_link_health.py --run-dir "$RUN"
```

Claim-grade localization requires:

- `/apollo/localization/pose` channel evidence with acceptable rate, monotonic
  timestamps, monotonic sequence, and bounded gaps.
- `header.timestamp_sec` and `measurement_time` both use simulation time, with
  p95 delta no greater than `1 ms`.
- `header.frame_id=map`.
- an explicitly configured CARLA world to Apollo map transform.
- position generated from verified rear-axle / VRP evidence, not raw CARLA
  actor origin.
- truthful heading source metadata and RFU-to-ENU quaternion decoded heading
  p95 difference below `1e-3 rad`.
- velocity, acceleration, angular velocity, and VRF fields with declared frame
  and unit semantics.
- chassis speed and localization speed consistency within configured
  tolerance.
- Apollo HDMap or reference-line projection evidence when a scenario uses lane,
  junction, curve, or stop-line claims, alongside `route_health.json` or
  `apollo_lateral_semantics_report.json` for curve conclusions.

Non-claim-grade localization modes include `confidence: assumed` vehicle
reference, using `configs/vehicles/ego_vehicle_reference.example.yaml` for a
hard-gate claim, stale timestamp republish counted as fresh samples, diagnostic
nearest-lane projection only, missing `apollo_hdmap_projection.jsonl`, and
missing `apollo_reference_line_contract_report.json`. These modes may still be
useful diagnostics, but they must keep the natural-driving verdict at
`insufficient_data`, `diagnostic_only`, or non-claim `warn`.

## Runtime Stats Refresh Boundary

When an online run contains `artifacts/cyber_bridge_stats.json` with canonical
localization metadata, `tools/analyze_apollo_localization_contract.py --run-dir`
may regenerate a claim-grade localization report even if an older placeholder
report under `analysis/localization_contract/` was stale. The analyzer should
prefer runtime fields such as `header.frame_id`, `measurement_time`,
`heading_source`, `orientation_convention`, verified vehicle reference, and
speed/yaw-rate consistency over legacy report placeholders.

This refresh only proves the GT localization contract layer. It does not prove
Apollo HDMap lane equivalence or Planning reference-line closure. If
`apollo_hdmap_projection.jsonl` is empty or missing, the run remains blocked at
the HDMap/reference-line layer, even when localization itself is `warn` or
`pass` with `claim_grade=true`.
