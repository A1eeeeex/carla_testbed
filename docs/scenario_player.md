# Fixed Scene Player

`FixedScenePlayer` provides deterministic playback for key non-ego scenario
actors. It is separate from ego control and from random background traffic.

## Ownership Boundary

- Ego control is owned by the selected backend, such as Apollo, Autoware, CARLA
  built-in control, or replay.
- Key scenario actors are owned by `fixed_scene_player`.
- Random background vehicles and walkers are owned by a traffic-flow provider.

Fixed-scene playback is scenario setup evidence. It is not a natural-driving
capability claim for the ego stack.

## Config Layers

Human-facing templates live in `configs/scenario_templates/` and use:

```yaml
schema_version: fixed_scene_template.v1
template: follow_stop
roles: {}
params: {}
success_criteria: {}
```

Concrete scenarios can embed a `fixed_scene` block, such as
`configs/scenarios/town01/follow_stop_097.yaml`.

Baguang fixed-scene scenarios are also available for quick lead-vehicle
diagnostics on `straight_road_for_baguang`:

- `configs/scenarios/baguang/follow_stop_static_300m.yaml`
  - ego target speed 80kph, lead vehicle stopped 300m ahead.
- `configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml`
  - ego and lead start near 40kph with a 20m gap; after 5s the lead ramps
    toward 70kph and the scene lasts until the lead reaches the current
    Baguang straight-road end.
- `configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml`
  - ego and lead start near 70kph with a 20m gap; after 5s the lead ramps down
    toward 40kph and the scene lasts until the lead reaches the current
    Baguang straight-road end.
- `configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml`
  - lead starts in the left adjacent lane at 35kph, ego starts in the right
    lane at 40kph, and the lead performs a cosine-eased 4s right cut-in when
    longitudinal gap reaches about 10m. The scene lasts until the lead reaches
    the current Baguang straight-road end.

These scenes include spawn feasibility checks. If the lead vehicle cannot be
placed at the requested ahead distance and lane, the fixed-scene contract must
fail rather than silently accepting a fallback spawn.

Templates compile to `fixed_scene_storyboard.v1`, usually written as:

```text
artifacts/fixed_scene_resolved.json
```

## Runtime Artifacts

The player and analyzers expect these artifacts:

- `artifacts/fixed_scene_resolved.json`
- `artifacts/fixed_scene_runtime_state.json` when a runtime adapter is used
- `artifacts/scenario_actor_trace.jsonl`
- `artifacts/scenario_phase_events.jsonl`
- `analysis/fixed_scene_contract/fixed_scene_contract_report.json`
- `analysis/scenario_actor_contract/scenario_actor_contract_report.json`

The trace rows record role, controller intent, target speed, observed speed,
position, route/lane fields when available, distance to ego, and applied
control if the runtime adapter supplies it.

## Offline Commands

Compile a template or concrete scenario:

```bash
python tools/compile_fixed_scene.py \
  --template configs/scenarios/town01/follow_stop_097.yaml \
  --out /tmp/fixed_scene
```

Analyze fixed-scene artifacts:

```bash
python tools/analyze_fixed_scene_contract.py \
  --storyboard /tmp/fixed_scene/fixed_scene_resolved.json \
  --trace <run>/artifacts/scenario_actor_trace.jsonl \
  --phase-events <run>/artifacts/scenario_phase_events.jsonl \
  --out <run>/analysis/fixed_scene_contract

python tools/analyze_scenario_actor_contract.py \
  --run-dir <run> \
  --out <run>/analysis/scenario_actor_contract
```

Missing trace or events produce `insufficient_data` or `warn`; they must not be
rewritten as ego autonomy success.

Run a local CARLA-only smoke when CARLA is already available:

```bash
python tools/run_fixed_scene_carla_smoke.py \
  --template configs/scenarios/town01/follow_stop_097.yaml \
  --run-dir runs/fixed_scene_follow_stop_smoke \
  --town Town01 \
  --duration-s 20
```

This command sets `ego_control_source=carla_builtin_smoke` and
`scenario_actor_control_source=fixed_scene_player`. It requires local CARLA and
does not start Apollo or Autoware.

Run the diagnostic builtin ego controller against a fixed scene:

```bash
python tools/run_builtin_ego_fixed_scene.py \
  --scenario configs/scenarios/baguang/follow_stop_static_300m.yaml \
  --run-dir runs/baguang_builtin_follow_stop_static_300m \
  --town straight_road_for_baguang \
  --duration-s 30
```

This command writes `artifacts/ego_control_trace.jsonl`, `timeseries.csv`,
`manifest.json`, `summary.json`, and the fixed-scene contract reports. It sets
`ego_control_source=carla_testbed_builtin_controller`. This is useful for
checking whether the scenario itself behaves as intended before connecting
Apollo or Autoware, but it is diagnostic-only and must not be used as a
natural-driving capability claim.

## CARLA Runtime Adapter

`carla_testbed.scenario_player.CarlaFixedSceneRuntime` is the first runtime
adapter. It uses lazy CARLA access and can also be tested with fake CARLA-like
objects. The adapter currently supports the follow-stop essentials:

- spawn non-ego roles such as `lead_vehicle`
- write `fixed_scene_resolved.json`
- write `fixed_scene_runtime_state.json`
- call `FixedScenePlayer.tick()`
- apply simple longitudinal controls for `follow_route`, `brake_to_stop`, and
  `hold_stop`
- apply diagnostic scripted lane-change motion for non-ego cut-in/cut-out
  actors, using smooth lateral interpolation rather than instant teleport
- record scenario actor trace and phase events

The runtime adapter intentionally does not control ego. It also does not yet
implement claim-grade lane-change control for cut-in/cut-out. The scripted
lane-change support is scenario playback evidence only.

For CARLA-only diagnostic runs, ego control is provided by
`SimpleAccRouteFollowerController` through the `carla_builtin` backend facade.
The controller is deliberately small: route-heading/cross-track feedback plus
ACC-style lead-gap speed control. It exists to validate scene playback quickly,
not to replace Apollo or Autoware in capability evaluation.

Expected integration order inside an online runner is:

```text
world tick / frame context ready
fixed_scene_runtime.tick()
traffic_flow.tick()
ego_backend.step()
recorders.on_frame()
evidence.collect()
```

## Gate And Perception Linkage

When a scenario enables `fixed_scene`, the platform evidence resolver requires:

- `analysis/fixed_scene_contract/fixed_scene_contract_report.json`
- `analysis/scenario_actor_contract/scenario_actor_contract_report.json`

For Apollo or Autoware diagnostic/claim runs, fixed-scene actors also require
obstacle GT evidence, and prediction evidence where applicable. A lead vehicle
or cut-in vehicle that exists only in CARLA but is missing from
`/apollo/perception/obstacles` or the Autoware equivalent invalidates the
scenario as autonomy evidence.

`scenario_validation` checks scene playback and actor behavior. It does not
require Apollo/Autoware link health. `claim_natural_driving` may consume the
same fixed-scene reports, but fixed-scene pass alone is never sufficient for an
ego natural-driving pass; that claim still requires `natural_driving_report.json`
and the broader Apollo/Autoware evidence chain.
