# Apollo Reference Chain

`configs/reference/apollo_reference_chain.yaml` is the canonical expected
contract for an official or mature Apollo-CARLA bridge. It complements
`configs/algorithms/apollo_inventory.yaml`.

The inventory says Apollo is a modular stack. The reference chain says which
modules, channels, artifacts, and replacement contracts are required before a
CARLA truth-input run can support an Apollo capability claim.

This document is not a statement that the current project has passed every
module. It is the checklist used to decide what evidence is still missing.

## Why This Exists

Apollo planning and control are not isolated functions. A claim-grade
closed-loop run must prove the full dependency chain:

- HDMap and routing provide valid route/reference-line context.
- Localization and chassis publish Apollo-compatible state.
- Obstacles, prediction, and traffic-light perception are present or explicitly
  replaced under a documented contract.
- Native Apollo planning publishes `/apollo/planning`.
- Native Apollo control publishes `/apollo/control`.
- The vehicle interface maps raw Apollo control to CARLA actuation with
  raw/mapped/applied/vehicle-response evidence.
- CyberRT channel evidence exists; Dreamview alone is operator evidence.

## Reference Modules

The YAML blueprint requires these modules:

- `hdmap`
- `routing`
- `localization`
- `chassis`
- `perception_obstacles`
- `prediction`
- `traffic_light_perception`
- `planning`
- `control`
- `vehicle_interface`
- `cyberrt`
- `dreamview`

Each module declares:

- official role
- required inputs and expected outputs
- required and optional CyberRT channels
- required artifacts and native evidence artifacts
- whether GT replacement is allowed
- bypass policy and conditions
- downstream dependents
- hard-gate capability requirements

## Replacement Boundaries

`hdmap` cannot be replaced by CARLA GT. Apollo route, junction, curve, and
traffic-light claims require Apollo HDMap/reference-line/signal evidence.

`routing` cannot be silently bypassed. A route follower or testbed trajectory is
not native Apollo routing evidence.
Claim-grade routing also requires `apollo_route_contract_report.json`: the
scenario route length/start/goal/lane identity must be compatible with the
Apollo routing response and route segment evidence. The preferred evidence is
`artifacts/routing_response_decoded.json` or
`artifacts/routing_response_decoded.jsonl`, written from the actual
`/apollo/routing_response`. Planning-derived route summaries are useful
diagnostic fallback but cannot alone support a claim-grade route pass. A
routing response that is much longer than the scenario route, or that uses an
incompatible lane/window sequence, blocks route establishment before
planning/control behavior is interpreted.

`localization` may be replaced by CARLA GT only with
`localization_contract_report.json`. The report must cover sim time, frame
transform, VRP/rear-axle position, heading/quaternion consistency, velocity,
yaw-rate, and route/lane projection evidence.

`chassis` may be replaced by CARLA GT only with `chassis_contract_report.json`
or equivalent channel/control feedback evidence.

`perception_obstacles` may be replaced by CARLA actor GT only with
`obstacle_gt_contract_report.json` or equivalent obstacle debug evidence.

`prediction` must not silently disappear. If a scenario bypasses prediction, it
needs `prediction_evidence_report.json` and an explicit `bypass_reason`.

Prediction is an explicit gate, not a background field. The allowed modes are:

- `native_observed`: `/apollo/prediction` has messages and channel evidence.
- `bypassed_with_gt_obstacles`: allowed only with a scenario-scoped
  `bypass_reason`; static lane-keep diagnostics may warn, but natural-driving
  closed-loop claims require `native_observed` prediction unless an explicit
  scenario override downgrades the claim boundary. Dynamic obstacle, junction,
  traffic-light, and closed-loop claims remain blocked by silent bypass.
- `missing`: no prediction channel and no acceptable bypass evidence.
- `not_required_for_case`: the case explicitly declares prediction is not part
  of the evaluated contract.
- `unknown`: not claim-grade and must not pass chain completion.

`/apollo/perception/obstacles` is not `/apollo/prediction`; obstacle messages do
not prove prediction availability.

`traffic_light_perception` may be replaced by CARLA actual traffic-light state,
but claim-grade traffic-light scenarios require signal/stop-line mapping,
CyberRT publish evidence, planning-consumed evidence, and observed behavior
evidence.

`planning` must be native Apollo. Testbed trajectories, dummy lateral control,
or route followers are blocking assists.

`control` must be native Apollo. Bridge-applied CARLA control is vehicle
interface evidence, not Apollo Control evidence.

`vehicle_interface` may use CARLA `VehicleControl`, but it must preserve
raw/mapped/applied/vehicle-response evidence and cannot hide assist behavior.

`cyberrt` evidence should include `channel_stats.json`, CyberRT logs, or
`cyber_record`. Summary success or Dreamview video alone is not enough.

`dreamview` is operator evidence. It is useful for demos and debugging, but it
does not prove planning/control health.

## GT Replacement Matrix

`configs/reference/apollo_gt_replacement_matrix.yaml` records the current
replacement status for the `apollo_ported_carla_gt` variant.

The reference chain defines the expected contract. The replacement matrix
answers a different question: for each reference module, is the current project
using native Apollo, CARLA GT replacement, a mock, a bypass, a missing path, or
operator-only evidence?

GT replacement is not omission. If CARLA truth replaces an upstream Apollo
module output, the replacement must provide the same downstream contract and
must have an evidence report proving that contract. For example:

- `localization` may be `gt_replaced`, but it needs
  `localization_contract_report.json`.
- `chassis` may be `gt_replaced`, but it needs a chassis contract report or
  equivalent channel/control feedback evidence.
- `perception_obstacles` may be `gt_replaced`, but it needs
  `obstacle_gt_contract_report.json` or equivalent obstacle debug artifacts.
- `traffic_light_perception` may be `gt_replaced`, but claim-grade traffic-light
  behavior needs signal/stop-line mapping, publish evidence, planning-consumed
  evidence, and behavior-observed evidence.
- `prediction` must not be silently unknown. If bypassed for a diagnostic
  scenario, it needs `prediction_evidence_report.json` and `bypass_reason`.
- `planning`, `control`, and `hdmap` cannot be marked as GT-replaced.
- `vehicle_interface` may be `carla_replaced`, but only with
  raw/mapped/applied/vehicle-response evidence.

The matrix intentionally does not mark the current truth-input stack as all
pass. Modules with `current_evidence_status=insufficient_data` or
`hard_gate_eligible=false` block their listed capabilities until the required
artifacts exist.

`configs/reference/apollo_link_evidence_index.yaml` is the companion runtime
evidence checklist. The replacement matrix answers "what is native, GT-replaced,
or bypassed by design"; the evidence index answers "which artifacts must exist
for a reviewer to inspect this run." Neither file can turn a run into a
natural-driving pass without `natural_driving_report.json` and the per-link
contract reports.

## Machine Checks

Load and validate the reference chain:

```bash
python - <<'PY'
from carla_testbed.algorithms.reference_chain import (
    load_apollo_reference_chain,
    required_modules_for_capability,
)

chain = load_apollo_reference_chain("configs/reference/apollo_reference_chain.yaml")
print([m["name"] for m in required_modules_for_capability(chain, "traffic_light")])
PY
```

Run the CI-safe tests:

```bash
python -m pytest tests/test_apollo_reference_chain.py -q
python -m pytest tests/test_gt_replacement_matrix.py -q
python -m pytest tests/test_apollo_prediction_evidence.py -q
```

The loader does not import CARLA, ROS2, CyberRT, or Apollo protobuf. It is safe
for offline CI and external review.

Inspect currently blocked capabilities from the replacement matrix:

```bash
python - <<'PY'
from carla_testbed.algorithms.gt_replacement_matrix import (
    blocked_capabilities,
    load_gt_replacement_matrix,
)

matrix = load_gt_replacement_matrix("configs/reference/apollo_gt_replacement_matrix.yaml")
print(blocked_capabilities(matrix))
PY
```

Generate prediction evidence for a run:

```bash
python tools/analyze_apollo_prediction_evidence.py \
  --run-dir <run_dir> \
  --out <run_dir>/analysis/prediction_evidence
```

By default this analyzer also reads
`configs/reference/apollo_gt_replacement_matrix.yaml` so static lane-keep runs can
record the project-scoped `bypassed_with_gt_obstacles` reason without pretending
that `/apollo/prediction` was observed. Use `--no-replacement-matrix` when testing
the negative case where no explicit bypass evidence should be available. Dynamic
obstacle, junction, and traffic-light claims still require native prediction or
an explicit scenario override; `/apollo/perception/obstacles` is not counted as
`/apollo/prediction`.

Generate module-consumption evidence for a run:

```bash
python tools/analyze_apollo_module_consumption.py \
  --run-dir <run_dir> \
  --out <run_dir>/analysis/apollo_module_consumption
```

This report is the explicit bridge-published versus Apollo-consumed check. It
uses planning materialization, Planning debug summaries, routing/control debug
rows, topic publish rows, prediction evidence, and Apollo logs to detect input
timeouts, reference-line provider failures, prediction-not-ready messages, and
empty Planning attribution. Bridge-side GT publish evidence alone does not
prove that Planning consumed localization, chassis, obstacles, prediction, or
routing inputs.

Generate route-contract evidence for a run:

```bash
python tools/analyze_apollo_route_contract.py \
  --run-dir <run_dir> \
  --frame-transform configs/town01/apollo_frame_transform.example.yaml \
  --out <run_dir>/analysis/apollo_route_contract
```

The route contract is the bridge between scenario intent and Apollo Routing
output. Coordinate comparison must happen in Apollo map frame: the report keeps
raw scenario CARLA XY as `scenario_*_xy_carla`, transformed scenario XY as
`scenario_*_xy_apollo`, Apollo Routing XY as `apollo_*_xy`, and records
`comparison_frame` plus `transform_source`. If the transform is missing, XY
agreement is `insufficient_data`/warning evidence, not a hard mismatch.
Length and lane-sequence mismatches remain hard route-contract failures.
When present, `routing_response_decoded` is consumed before Planning-derived
route summaries so the route identity contract is anchored in Apollo's Routing
response rather than inferred from downstream Planning debug. Missing decoded
RoutingResponse evidence keeps the route layer diagnostic-only/warn-level.

`apollo_route_contract_report.json` also separates `startup_route_contract`
from `claim_route_contract`. An ego-seed startup route can be useful diagnostic
evidence that Routing answered, but it cannot materialize a 230 m scenario
claim route. If only the startup route is observed, downstream gates must
surface `claim_route_not_materialized` before interpreting Planning, Control,
or actuation metrics as natural driving.

The report uses an explicit route phase taxonomy: `startup`, `long_goal`,
`claim`, and `unknown`. It records `raw_routing_phase` before compatibility
resolution plus the configured scenario route, last routing request, last
routing response, and latest Planning active route segment. A long goal or
multi-road Apollo response can be promoted to `claim` only when its length,
lane signature, start/goal, and snap/Frenet compatibility match the configured
scenario route. Otherwise the route layer must remain failed with
`route_identity_inconsistent`, `long_goal_not_compatible_with_scenario_route`,
or `claim_route_not_materialized`.

`apollo_link_health`, `apollo_chain_completion`, `apollo_module_consumption`,
and `natural_driving_report.json` consume this report before route
establishment. If the scenario route is about 230 m but Apollo reports a short
startup route or a multi-road 648 m routing response, the route layer must fail
with `claim_route_not_materialized` or `apollo_routing_length_mismatch` rather
than allowing later control metrics to be interpreted as natural driving.
`apollo_module_consumption_report.json` may still report that Planning consumed
some routing response; that is module-consumption evidence, not proof that the
configured scenario route was consumed.

For review and CI-friendly postprocess, route identity and routing are also
available as standalone reports:

```bash
python tools/analyze_route_identity.py \
  --run-dir <run_dir> \
  --frame-transform configs/town01/apollo_frame_transform.example.yaml \
  --out <run_dir>/analysis/route_identity

python tools/analyze_routing_contract.py \
  --run-dir <run_dir> \
  --frame-transform configs/town01/apollo_frame_transform.example.yaml \
  --out <run_dir>/analysis/routing_contract
```

`route_identity_report.json` is the route oracle surface. It keeps configured
scenario route identity, Apollo Routing lane sequence, Planning reference-line
lane sequence, start/goal poses, and length agreement in one artifact. It is
allowed to fail before Planning or Control are interpreted. `routing_contract`
is narrower: it checks routing request/response compatibility and whether the
claim route, not only a startup or helper route, materialized.

Planning materialization reports keep time domains explicit. Latency or
freshness metrics are valid only when both sides use the same domain
(`sim_time` to `sim_time`, or wall time to wall time). Mixed-domain latency,
zero as-of join coverage, or missing topic rows produces `insufficient_data`
freshness attribution; analyzers must not report localization/chassis stale
counts as zero when freshness evidence is unavailable.
Natural-driving summaries therefore record both overall Planning materialization
and the filtered claim window. `planning_nonempty_ratio_filtered` can explain
startup dilution, while `planning_nonempty_ratio_overall` and
`route_establishment_source=planning_materialization` preserve the route
establishment evidence. If planning materialization fails, the claim fails even
when a filtered reference-line window looks healthy.

## Chain Completion

`carla_testbed.analysis.apollo_chain_completion` is the reference-chain-aware
completion layer. It deliberately reuses `apollo_link_health` layer summaries
instead of creating a second competing health scorer.

The chain completion report answers:

- which reference modules have native, GT-replaced, bypassed, missing, or
  operator-only evidence;
- for each module, the project matrix status, run-level evidence status,
  run-level claim-grade flag, and effective status used by gates;
- which required artifacts were observed or missing;
- which capabilities are blocked by missing or insufficient module evidence;
- whether truth-input closed-loop and unassisted natural-driving claims are
  currently allowed.

The top-level `missing_required_evidence` list is scoped to the run's target
capability. For example, a `lane_keep` run may still show
`traffic_light_perception` as a non-claim-grade module in `module_statuses`, but
traffic-light-only artifacts are not listed as missing lane-keep evidence. Use
`module_statuses` for the full platform audit and `missing_required_evidence`
for the current capability blocker list.

Run it on an existing run directory:

```bash
python tools/analyze_apollo_chain_completion.py \
  --run-dir <run_dir> \
  --reference configs/reference/apollo_reference_chain.yaml \
  --replacement configs/reference/apollo_gt_replacement_matrix.yaml \
  --out <run_dir>/analysis/apollo_chain_completion
```

Outputs:

- `apollo_chain_completion_report.json`
- `apollo_chain_completion_summary.md`
- compatible `apollo_link_health_report.json` and summary when requested by the
  wrapper

Important boundaries:

- `route_health.json` can prove CARLA route geometry, but it cannot by itself
  prove Apollo HDMap/reference-line completion.
- `summary.json` success cannot make chain completion pass.
- traffic-light behavior cannot be used to infer Planning consumed the
  traffic-light message; planning-consumed evidence must be explicit.
- missing artifacts become `insufficient_data` or `missing`, not pass.
- Route establishment is its own chain stage. A run with missing or failed
  `route_establishment` evidence should attribute the failure to planning
  materialization/routing consumption before blaming control mapping or total
  bridge breakage.
- Apollo HDMap projection must be official `source=apollo_hdmap_api` evidence
  with sufficient sample, sim-time, route-s/projection-s, lane-id, and
  map-identity coverage. Sparse startup-only projection rows cannot upgrade
  reference-line or localization evidence to claim-grade.
- Background traffic or pedestrian actors require obstacle GT linkage. For
  Apollo/Autoware runs, each spawned background actor id from
  `traffic_flow_manifest.json` must appear in `obstacle_gt_contract_report.json`
  with stable perception id and correct object type before dynamic behavior can
  be interpreted.
- Module statuses must use module-specific evidence. For example, if
  `apollo_channel_health_report.json` fails only because `/apollo/planning` has
  a large gap, the `chassis` module must remain governed by the chassis channel
  sub-result rather than inheriting the overall channel-health failure. The
  overall channel failure still blocks the CyberRT/channel layer and downstream
  closed-loop claim until resolved.
- Current CARLA apply evidence may appear as `control_apply_trace.jsonl` rather
  than the older `direct_bridge_control_apply.jsonl`; both are row-level vehicle
  interface evidence, while `control_health_report.json` still decides whether
  cadence/apply/mapping is pass, warn, or fail.

## Claim Boundary

The reference chain is an expected contract. It does not downgrade current
failures, and it must not be used to claim that Apollo already drives Town01
naturally.

Capability claims still require concrete artifacts such as
`natural_driving_report.json`, `localization_contract_report.json`,
`apollo_reference_line_contract_report.json`, `apollo_control_handoff_report.json`,
`control_health_report.json`, traffic-light/obstacle contract reports when
applicable, and a clean `assist_ledger`.
