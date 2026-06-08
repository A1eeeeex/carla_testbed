# Scenario Validation Gate

`scenario_validation` checks CARLA scenario structure, recording prerequisites,
basic route/control artifacts, and optional traffic-flow contracts without
requiring Apollo or Autoware analyzers.

Example dry-run plan:

```bash
python -m carla_testbed plan \
  --platform carla_builtin \
  --algorithm simple_route_follower \
  --scenario town01/lane_keep_097 \
  --traffic town01/random_tm_2 \
  --record demo \
  --gate scenario_validation \
  --out /tmp/town01_random_tm_2.plan.yaml
```

When background vehicles are enabled, evidence resolution requires
`traffic_flow_contract`. The gate verifies that requested background vehicles
spawned, TM synchronous mode matches the world, and ego is not registered to
Traffic Manager.

When background walkers are enabled, evidence resolution requires
`pedestrian_flow_contract`. The gate verifies that requested walkers spawned,
WalkerAIController instances started, role names are unique, and ego/scenario
actors were not registered as background walkers.

This gate is for scenario health. It must not be used as Apollo/Autoware
natural-driving evidence.
