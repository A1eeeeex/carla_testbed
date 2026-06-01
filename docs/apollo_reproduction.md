# Apollo Reproduction Levels

This document defines the Apollo reproduction ladder used before treating
Town01 or other CARLA scenarios as algorithm benchmarks. The goal is to avoid
confusing environment, adapter, bridge, or actuation problems with Apollo
algorithm capability.

Machine-readable report:

- `configs/algorithms/apollo_reproduction_report.example.yaml`
- schema: `algorithm_reproduction.v1`
- loader: `carla_testbed.algorithms.reproduction.load_reproduction_report`

Machine-readable gate:

- `configs/algorithms/apollo_reproduction_gate.yaml`
- schema: `reproduction_gate.v1`
- evaluator: `carla_testbed.algorithms.reproduction_gate.evaluate_reproduction_gate`

## Why Not Tune First

Do not start by changing Apollo planning/control parameters. A closed-loop
failure can come from environment drift, map mismatch, frame/time conversion,
routing contract, bridge transport, control mapping, latency, or CARLA
actuation. Tuning Apollo before these layers are frozen creates a black box:
the result may be better, but it stops being clear whether the open-source
algorithm improved or the platform moved underneath it.

Use the variant manifest from `docs/apollo_algorithm_inventory.md` to separate:

- `upstream`: Apollo code/config without CARLA testbed tuning.
- `ported_carla_gt`: Apollo modules driven by CARLA ground-truth adapter inputs.
- `tuned_town01`: Town01-specific patch/config/calibration experiments.

`tuned_town01` must record `tuning_reason` and patch paths. It must not be
reported as upstream Apollo.

## Level Summary

| Level | Purpose | Pass means | Failure attribution |
| --- | --- | --- | --- |
| L0 environment frozen | Freeze environment, variant, config diff, Docker, map, and no-patch/patch state. | The run is reproducible enough to inspect. | Environment or reproducibility gap. |
| L1 upstream demo or record replay | Prove upstream/replay baseline is inspectable outside CARLA closed-loop claims. | Dreamview/record/channel evidence exists. | Upstream runtime or replay setup. |
| L2 module golden replay | Replay known inputs and compare module output digests. | Planning/control digests match expected replay. | Apollo module replay mismatch. |
| L3 CARLA-to-Apollo adapter contract | Validate frame, time, routing, and channel contracts. | Inputs to Apollo are contract-valid. | Adapter/input contract, not Apollo capability. |
| L4 shadow mode | Run Apollo outputs against route evidence without depending on CARLA actuation. | Planning/control semantics are interpretable against route health. | Input contract or planning semantics issue. |
| L5 closed loop | Apply Apollo control through bridge to CARLA and evaluate route health. | Closed-loop artifacts exist and satisfy the run-specific criteria. | If L4 passed, check control bridge, actuation, or latency first. |

## Required Artifacts

### L0 Environment Frozen

- `algorithm_variant_manifest.yaml`
- `environment_manifest.json`
- `apollo_config_diff.patch` or an explicit no-patch marker

L0 should also freeze Docker image digest, Apollo git commit, map hashes,
vehicle config hash, bridge config hash, calibration profile hash, and the
stable `variant_id`.

### L1 Upstream Demo Or Record Replay

- `record_info.txt`
- `dreamview_screenshot.png`
- `cyber_channel_stats.json`

This level proves the Apollo runtime or replay evidence is inspectable. It does
not prove CARLA closed-loop behavior.

### L2 Module Golden Replay

- `apollo_output.record`
- `planning_digest.json`
- `control_digest.json`
- `replay_report.json`

Use this level to detect module-level changes before involving CARLA
actuation.

The replay digest comparator is the first L2 tool:

- checker: `carla_testbed.algorithms.replay_digest.compare_replay_digest`
- CLI: `tools/analyze_apollo_replay_digest.py`
- input: JSON digest exported from replay output
- output schema: `replay_comparison_report.v1`

Current CI support is intentionally limited to synthetic digest JSON. The tool
does not parse binary CyberRT record files and does not start Apollo. For real
L2 evidence, export planning/control/localization/chassis summaries with local
Apollo/CyberRT tooling, then compare those JSON digests against a golden
digest.

Digest pass means replay outputs stayed within configured metric tolerances. It
does not prove CARLA closed-loop capability, route health, actuation correctness
or full native perception.

Example:

```bash
python tools/analyze_apollo_replay_digest.py \
  --golden tests/fixtures/apollo/replay_digest_golden.json \
  --candidate tests/fixtures/apollo/replay_digest_candidate.json \
  --out /tmp/replay_digest_report
```

### L3 CARLA-To-Apollo Adapter Contract

- `adapter_contract_report.json`
- `frame_transform_report.json`
- `routing_contract_report.json`
- `channel_stats.json`

If L3 fails, do not attribute the result to Apollo algorithm capability. Fix
the adapter contract first.

The channel contract checker is the first L3 tool:

- contract: `configs/algorithms/apollo_channel_contract.yaml`
- checker: `carla_testbed.algorithms.channel_contract.check_channel_stats_file`
- input: synthetic or real `channel_stats.json`
- output schema: `adapter_contract_report.v1`

It checks the core Apollo channels:

- `/apollo/localization/pose`
- `/apollo/canbus/chassis`
- `/apollo/perception/obstacles`
- `/apollo/routing_response`
- `/apollo/planning`
- `/apollo/control`

The checker verifies presence, approximate rate, timestamp monotonicity,
sequence monotonicity, stale messages, and max message gap. It does not prove
semantic correctness of route geometry, localization reference point, planning
quality, or control mapping. It only proves that the adapter/channel contract is
healthy enough for later L4/L5 interpretation.

Missing `localization`, `chassis`, `planning`, or `control` is a contract
failure. Missing `perception_obstacles` can be a warning in GT-less replay
contexts, depending on the contract config.

### L4 Shadow Mode

- `shadow_mode_report.json`
- `planning_vs_route.csv`
- `control_shadow_timeseries.csv`
- `route_health.json`

If L4 fails, the conclusion should be
`input_contract_or_planning_semantics_issue`. Do not jump directly to actuation
or calibration.

The shadow-mode analyzer is the first L4 tool:

- config: `configs/algorithms/apollo_shadow_mode_check.yaml`
- analyzer: `carla_testbed.algorithms.shadow_mode.analyze_shadow_mode_timeseries`
- input: CSV timeseries with route-health/P0 and Apollo planning/control fields
- output schema: `apollo_shadow_mode_report.v1`

Shadow mode means Apollo receives CARLA GT localization/chassis/obstacles and
routing, then emits planning/control, but those control outputs are not applied
to CARLA. This separates algorithm-output semantics from control bridge,
latency, and physical actuation.

The analyzer reports:

- planning/control availability and approximate rates
- lateral and heading alignment against the route
- route curvature versus raw steering correlation
- raw steer/throttle/brake availability
- high-steer spikes and brake/throttle conflicts
- matched-point and target-point anomalies when those fields exist

Missing Apollo matched/target fields must not crash the analyzer. They make the
report `insufficient_data` unless the configured evidence is otherwise enough.
Shadow-mode pass is still not a closed-loop pass; it only says Apollo output is
reasonable before actuation.

### L5 Closed Loop

- `summary.json`
- `manifest.json`
- `timeseries.csv` or `timeseries.jsonl`
- `route_health.json`
- optional `control_actuation_report.json`

L5 proves a closed-loop run under a specific variant and route set. It does not
prove full Apollo native perception unless the variant inventory says native
perception is enabled and has its own evidence.

## Validator Rules

The reproduction validator enforces:

- L5 `pass` requires L0-L4 to be `pass` or explicitly `waived`.
- L3 `fail` cannot use `apollo_algorithm_capability` as the failure reason.
- L4 `fail` must use
  `input_contract_or_planning_semantics_issue` before actuation is considered.
- L4 `pass` + L5 `fail` should prioritize control bridge, actuation, and
  latency checks.
- Tuned Town01 reports must include `tuning_patch_path` and `tuning_reason`.

## Gate Into Town01 Evidence

Before treating a Town01 closed-loop result as an algorithm capability claim,
run the reproduction gate:

```python
from carla_testbed.algorithms.reproduction_gate import evaluate_reproduction_gate
```

The gate connects the reproduction ladder to the three Town01 evidence
surfaces:

- route-health: required before curve lateral semantics conclusions.
- `ros2_gt` vs `carla_direct` A/B: required before direct-transport improvement
  claims.
- calibration: optional for general evidence, but required before changing
  `steer_scale` or promoting physical mapping.

Gate rules:

- If L0-L3 are not `pass` or explicitly waived, the result is blocked and must
  not be called an Apollo algorithm limitation.
- If L4 is not `pass` or waived, closed-loop capability claims are blocked; fix
  route/reference-line/planning semantics first.
- If L5 fails while L4 passed and control-actuation evidence is absent, attach a
  `control_actuation_report` before claiming algorithm limitation.
- Missing route-health prevents curve semantics conclusions.
- Missing A/B report prevents `carla_direct` improvement claims.
- Missing calibration is a warning unless the requested claim changes
  `steer_scale` or physical mapping.

## Local Validation Commands

CI-friendly schema validation:

```bash
python -m pytest tests/test_apollo_reproduction_report.py -q
```

Inspect the example report:

```bash
python - <<'PY'
from carla_testbed.algorithms.reproduction import load_reproduction_report, validate_reproduction_report
r = load_reproduction_report("configs/algorithms/apollo_reproduction_report.example.yaml")
print(validate_reproduction_report(r).status)
PY
```

Real L1-L5 evidence requires local Apollo/CARLA runs and should attach the
artifacts listed above. Do not use short-window transport positives as proof
that the full reproduction ladder has passed.

## Unified Offline Check CLI

`tools/check_apollo_reproduction.py` is a lightweight wrapper around the
inventory, variant, reproduction report, L3 channel contract, L2 replay digest,
L4 shadow mode, and reproduction gate checks. It only consumes existing
JSON/YAML/CSV artifacts. It does not start Apollo, does not start CARLA, and
does not parse binary CyberRT records.

Minimal gate check:

```bash
python tools/check_apollo_reproduction.py \
  --inventory configs/algorithms/apollo_inventory.yaml \
  --variant configs/algorithms/apollo_variant.carla_gt.example.yaml \
  --reproduction-report configs/algorithms/apollo_reproduction_report.example.yaml \
  --out /tmp/apollo_repro_check
```

Optional L2/L3/L4 artifact check:

```bash
python tools/check_apollo_reproduction.py \
  --inventory configs/algorithms/apollo_inventory.yaml \
  --variant configs/algorithms/apollo_variant.carla_gt.example.yaml \
  --reproduction-report configs/algorithms/apollo_reproduction_report.example.yaml \
  --channel-stats tests/fixtures/apollo/channel_stats_valid.json \
  --replay-golden tests/fixtures/apollo/replay_digest_golden.json \
  --replay-candidate tests/fixtures/apollo/replay_digest_candidate.json \
  --shadow-timeseries tests/fixtures/apollo/shadow_mode_timeseries.csv \
  --out /tmp/apollo_repro_check \
  --allow-warn-as-success
```

Outputs:

- `apollo_reproduction_check.json`
- `apollo_reproduction_check.md`
- optional generated subreports, such as `adapter_contract_report.json`,
  `replay_digest/replay_comparison_report.json`, and
  `shadow_mode/shadow_mode_report.json`

Exit code policy:

- `pass`: returns 0.
- `warn`: returns non-zero by default; add `--allow-warn-as-success` for CI jobs
  that accept missing optional route-health/A-B/calibration artifacts.
- `fail` or `blocked`: returns non-zero.
