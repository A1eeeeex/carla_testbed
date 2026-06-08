# Traffic Flow

Traffic flow is modular background traffic. Vehicles use CARLA Traffic Manager.
Pedestrians use CARLA `WalkerAIController`. Both are separate from ego control
and from scripted scenario actors.

Control-source boundary:

- Ego: Apollo, Autoware, or `carla_builtin`.
- Scripted scenario actors: `ScenarioBehaviorPlugin` or equivalent templates.
- Background vehicles: CARLA Traffic Manager, with `background_vehicle_*`
  role names.
- Background pedestrians: CARLA WalkerAIController, with
  `background_walker_*` role names.

Traffic Manager vehicles are not goal-oriented replay actors. CARLA TM
dynamically generates routes and may choose junction directions at runtime, so
it is suitable for random background traffic, not for fixed lead/cut-in
scenario templates.

WalkerAIController pedestrians are random navigation actors. They are suitable
for background crowd/load demos and basic scenario validation. They are not a
scripted pedestrian crossing or cut-in actor; deterministic crossing behavior
belongs in a future `ScenarioBehaviorPlugin`.

Synchronous runs should configure Traffic Manager to follow CARLA world
synchronous mode. Deterministic traffic must record and reapply the seed for
each world reload.

Required artifacts when traffic is enabled:

- `artifacts/traffic_flow_manifest.json`
- `artifacts/traffic_flow_events.jsonl`
- `artifacts/traffic_spawn_candidates.jsonl` for vehicle Traffic Manager flow
- `artifacts/walker_spawn_candidates.jsonl` for WalkerAIController flow
- `analysis/traffic_flow_contract/traffic_flow_contract_report.json` when
  background vehicles are enabled
- `analysis/pedestrian_flow_contract/pedestrian_flow_contract_report.json` when
  background pedestrians are enabled

`traffic_flow_contract` passing only proves background traffic was generated
reproducibly, controlled by Traffic Manager, and did not register ego or
scripted scenario actors. It does not prove Apollo or Autoware natural driving.
`pedestrian_flow_contract` passing only proves background walkers were spawned,
AI controllers started, and actor-control boundaries were recorded. It does not
prove Apollo/Autoware pedestrian perception, prediction, planning, or avoidance.
`analysis/gate/gate_report.json` echoes `ego_control_source`,
`scenario_actor_control_source`, `background_traffic_control_source`, and
`background_walker_control_source` so actor-control domains remain auditable.

Available Town01 profiles:

- `town01/random_tm_1`, `town01/random_tm_2`: vehicle-only Traffic Manager.
- `town01/random_walkers1`, `town01/random_walkers2`: pedestrian-only
  WalkerAIController.
- `town01/random_tm2_walkers2`: mixed Traffic Manager vehicles plus
  WalkerAIController pedestrians.

If walkers are enabled and an Apollo/Autoware obstacle-behavior claim is made,
`obstacle_gt_contract_report.json` must include a pedestrian section. Missing
pedestrian obstacle evidence blocks dynamic pedestrian capability claims.

## Visual Demo Recording

Random traffic-flow demo video must not be accepted only because vehicles were
spawned. Before recording, the operator workflow should prewarm CARLA Traffic
Manager and verify that enough background actors moved by a configured
threshold. The helper below records a visual demo and writes movement evidence:

```bash
python tools/record_traffic_flow_demo.py \
  --vehicles 8 \
  --duration-s 30 \
  --min-moving-actors 3 \
  --min-displacement-m 8.0 \
  --out runs/traffic_flow_recording_<id>_normal
```

Expected outputs:

- `video/traffic_flow_demo_normal.mp4`
- `artifacts/traffic_flow_recording_status.json`
- `artifacts/traffic_flow_manifest.json`
- `artifacts/traffic_flow_events.jsonl`

`traffic_flow_recording_status.json.status=ready` requires both a non-empty
video and `recording_movement.status=pass`. This prevents the earlier bad demo
case where Traffic Manager actors existed but were stuck at road edges or
guardrails.
