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
  `bypass_reason`; static lane-keep diagnostics may warn, but dynamic obstacle,
  junction, traffic-light, and closed-loop claims remain blocked unless an
  explicit override is justified.
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

## Chain Completion

`carla_testbed.analysis.apollo_chain_completion` is the reference-chain-aware
completion layer. It deliberately reuses `apollo_link_health` layer summaries
instead of creating a second competing health scorer.

The chain completion report answers:

- which reference modules have native, GT-replaced, bypassed, missing, or
  operator-only evidence;
- which required artifacts were observed or missing;
- which capabilities are blocked by missing or insufficient module evidence;
- whether truth-input closed-loop and unassisted natural-driving claims are
  currently allowed.

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

## Claim Boundary

The reference chain is an expected contract. It does not downgrade current
failures, and it must not be used to claim that Apollo already drives Town01
naturally.

Capability claims still require concrete artifacts such as
`natural_driving_report.json`, `localization_contract_report.json`,
`apollo_reference_line_contract_report.json`, `apollo_control_handoff_report.json`,
`control_health_report.json`, traffic-light/obstacle contract reports when
applicable, and a clean `assist_ledger`.
