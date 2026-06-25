# Platform RunPlan

`RunPlan` is the new offline planning layer for making CARLA testbed runs
selectable by platform, algorithm, scenario, recording profile, and gate
profile. It is a migration surface, not a new runtime stack yet.

Current scope:

- Compile profile selections into `run_plan.v1`.
- List available platform / algorithm / scenario / recording / gate profiles.
- Compile a suite matrix into per-run `*.plan.resolved.yaml` files.
- Resolve backend facade contracts and recording profile metadata without
  importing runtime stacks.
- Build an evidence bundle and gate summary from existing run artifacts.
- Package small run evidence for review without copying large videos or bags by
  default.
- Resolve scenario/platform-specific evidence requirements and claim rules.
- Write conservative `run --plan --dry-run` runtime context artifacts.
- Keep existing Apollo/Town01 legacy runtime entrypoints unchanged.

Current non-scope:

- It does not start CARLA, Apollo, Autoware, ROS2, or CyberRT unless an
  operator explicitly enables guarded legacy dispatch with
  `CARLA_TESTBED_ALLOW_LEGACY_DISPATCH=1`.
- It does not replace `tools/run_town01_*` yet.
- It does not make a behavior or natural-driving claim.
- It does not infer missing evidence as success.

## Commands

List profile registry entries:

```bash
python -m carla_testbed list platforms
python -m carla_testbed list algorithms
python -m carla_testbed list scenarios
python -m carla_testbed list recording
python -m carla_testbed list recorders
python -m carla_testbed list gates
python -m carla_testbed list backends
```

Compile one plan:

```bash
python -m carla_testbed plan \
  --platform apollo_cyberrt \
  --algorithm apollo/apollo10_carla_gt \
  --scenario town01/lane_keep_097 \
  --record demo \
  --gate diagnostic \
  --out /tmp/plan.resolved.yaml
```

Show the backend launch description without starting runtime:

```bash
python -m carla_testbed plan \
  --platform apollo_cyberrt \
  --algorithm apollo/apollo10_carla_gt \
  --scenario town01/lane_keep_097 \
  --traffic none \
  --record claim \
  --gate claim_natural_driving \
  --show-launch
```

Compile a suite matrix:

```bash
python -m carla_testbed plan \
  --suite configs/suites/town01_natural_driving.platform.yaml \
  --out /tmp/town01_platform_plans
```

Dry-run a suite matrix and write a manifest plus matrix CSV:

```bash
python -m carla_testbed suite dry-run \
  --suite configs/suites/town01_natural_driving.platform.yaml \
  --out /tmp/town01_platform_suite
```

Preview typed RunPlan dispatch without starting runtime:

```bash
python -m carla_testbed run \
  --plan /tmp/town01_platform_suite/plans/<run_id>.plan.resolved.yaml \
  --plan-only
```

Write dry-run runtime context artifacts:

```bash
python -m carla_testbed run \
  --plan /tmp/town01_platform_suite/plans/<run_id>.plan.resolved.yaml \
  --run-dir /tmp/<run_id> \
  --dry-run
```

This writes `plan.resolved.yaml`, `launch_plan.json`, `manifest.json`,
`summary.json`, and `platform_execution_result.json`. The non-dry-run legacy
dispatch path is guarded; without `CARLA_TESTBED_ALLOW_LEGACY_DISPATCH=1` it
writes a blocked dispatch result instead of starting CARLA/Apollo/Autoware.

Dry-run the five Phase 1 P0 ApolloBackend vs PlanningControlBackend pairs:

```bash
python -m carla_testbed phase1 run-p0-matrix \
  --out /tmp/phase1_p0_matrix_dry \
  --dry-run
```

Attempt the same five P0 pairs online when the local CARLA/Apollo environment
is ready:

```bash
python -m carla_testbed phase1 run-p0-matrix \
  --out runs/phase1_p0_matrix/<id> \
  --timeout-s 180
```

For `carla_builtin` fixed-scene runs, the launch plan uses the first executable
Python found in `CARLA_TESTBED_CARLA_PYTHON`, `CARLA16_PYTHON`, or common local
`carla16` conda paths, then falls back to `python3`. Operators should set one
of those variables if their shell `python3` cannot `import carla`. The selected
interpreter is recorded in the launch-plan warnings.

Build evidence and gate reports from an existing run directory:

```bash
python -m carla_testbed analyze \
  --run-dir runs/<run_id> \
  --plan /tmp/town01_platform_suite/plans/<run_id>.plan.resolved.yaml
```

This writes:

- `analysis/evidence_bundle/evidence_bundle.json`
- `analysis/gate/gate_report.json`
- `analysis/gate/gate_summary.md`

Package review evidence without large runtime media by default:

```bash
python -m carla_testbed pack \
  --run-dir runs/<run_id> \
  --out /tmp/<run_id>_evidence.tar.gz
```

For `--profile claim`, the packager includes row-level JSONL evidence such as
`topic_publish_stats.jsonl`, `publish_gap_trace.jsonl`,
`control_apply_trace.jsonl`, and `planning_topic_debug.jsonl` when present. It
writes `package_manifest.json` inside the archive with included files, omitted
large artifacts, missing required row-level evidence, and
`claim_reproducibility_level`.

Apollo claim gates require separate GT localization and GT chassis contracts.
`localization_contract_report.json` proves pose/time/frame/VRP semantics;
`chassis_gt_contract_report.json` proves chassis channel, speed, driving mode,
gear, and error-code semantics. A generic channel-health pass is not enough for
either contract.

Traffic flow is a first-class RunPlan profile. Use `--traffic none` for no
background traffic or profiles such as `--traffic town01/random_tm_2` for
deterministic CARLA Traffic Manager background vehicles. When enabled, evidence
resolution requires `traffic_flow_contract`; this validates background traffic
setup only and does not count as Apollo/Autoware natural-driving evidence.

## Profile Boundary

Profiles are composable:

- `configs/platforms/` chooses the runtime platform such as `apollo_cyberrt`
  or `autoware_ros2`.
- `configs/algorithms/` chooses the algorithm variant within that platform.
- `configs/scenarios/` describes map, route, actors, requirements, and success
  intent.
- `configs/recording/` chooses neutral and platform-specific recorders.
- `configs/traffic/` chooses no background traffic or deterministic CARLA
  Traffic Manager profiles.
- `configs/gates/` chooses required analyzers and claim requirements.
- `configs/suites/` expands scenarios x platforms x algorithms x recording x
  gates into multiple RunPlans.

The current platform suite includes representative Town01 lane keeping, curve
diagnostic, junction, and traffic-light scenarios. Curve entries remain
diagnostic until route, localization, HDMap projection, reference-line, matched
point, target point, and control evidence are complete enough for a hard gate.

Recording profiles remain operator evidence. A `demo` profile can request
CARLA video, Dreamview, RViz, or rosbag artifacts, but recording alone is not a
natural-driving pass. Claim boundaries still require the existing evidence
chain: route health, channel health, localization contract, HDMap projection,
reference-line contract, planning materialization, control handoff/control
health, prediction/obstacle/traffic-light evidence when applicable, assist
ledger, and `natural_driving_report.json`.

Gate profiles are resolved through `carla_testbed.platform.evidence_resolver`.
The resolver combines platform, algorithm, scenario requirements, recording
profile, and gate profile into:

- required analyzers
- optional analyzers
- not-applicable analyzers
- structured claim rules

For example, an Autoware diagnostic RunPlan does not require
`apollo_link_health`, while an Apollo traffic-light claim RunPlan requires
traffic-light contract and behavior evidence. Structured gate rules check
specific metrics such as Planning non-empty trajectory ratio, route
establishment, HDMap projection claim grade, localization claim grade, control
source, and blocking assists. Report `status=pass` alone is not sufficient if a
rule metric fails.

## Migration Rule

Existing legacy tools remain compatibility entrypoints. New platform work
should compile or consume `RunPlan` first, then dispatch to a backend wrapper.
Do not add new one-off runtime logic to `tools/run_*.py` when the same choice
can be represented as a platform, algorithm, scenario, recording, or gate
profile.

`RunPlan` dispatch now goes through the package-level
`BackendRuntimeAdapter` / `ScenarioRunExecutor` surface:

- `python3 -m carla_testbed run --plan ... --plan-only` only previews the
  backend contract.
- `python3 -m carla_testbed run --plan ... --dry-run` writes runtime context
  artifacts without starting runtime.
- `python3 -m carla_testbed run --plan ... --run-dir runs/<id>` executes the
  resolved backend launch command, records PID/stdout/stderr/timeout under
  `execution/`, writes `preflight.json` plus
  `platform_execution_result.json`, records adapter cleanup status, and runs
  declared postprocess commands even after a failed runtime command when a
  runtime command was attempted. This is deliberate: failed Apollo/CARLA runs
  still need `phase1_status`, failure-reason, and artifact-completeness
  evidence.
- `python3 -m carla_testbed phase1 run-pair --scenario <case> --out <dir>`
  compiles a PlanningControlBackend plan and an ApolloBackend plan, executes
  them sequentially, and writes a generic ScenarioComparison.
- `python3 -m carla_testbed phase1 run-p0-matrix --out <dir>` runs the five
  Phase 1 P0 scenario pairs through the same pair-runner surface and writes
  `phase1_p0_matrix_manifest.json` plus `phase1_p0_matrix.csv`.

This is a delivery-surface milestone, not a behavior claim. A backend command
can still fail, timeout, or produce an invalid Phase 1 run. Invalid runs remain
setup/evidence failures and must not be counted as backend losses.

The P0 matrix runner is only orchestration evidence. It records per-case pair
manifests, run directories, exit codes, and comparison paths, and continues
across failed pairs by default. Behavior failures, invalid runs, and no-assist
Apollo boundaries must still be read from each pair's `phase1_status`,
`ScenarioComparison`, assist ledger, and backend-specific reports. A dry-run
matrix proves command/materialization coverage only; an online matrix proves
only that the selected pairs were attempted and artifacted.

The current `BackendRuntimeAdapter` executes exactly one runtime command per
LaunchPlan. If a backend emits multiple runtime commands, the adapter returns
`unsupported_multi_command` rather than silently executing only the first step.
Backends that need a real command pipeline must expose a single wrapper command
or implement a proper lifecycle pipeline before Phase 1 uses that plan online.
The Phase 1 pair runner no longer sets platform-level `legacy_dispatch=True`;
legacy/transition compatibility is represented inside the selected backend
LaunchPlan and adapter boundary.
The adapter result includes `command`, `cleanup`, and `postprocess` sections so
operators can distinguish process exit, stream/process cleanup, and evidence
generation outcomes.

Phase 1 currently has a deliberately narrow Apollo fixed-scene compatibility
exception for Baguang static follow-stop variants:
`baguang/follow_stop_static_300m` and
`baguang/follow_stop_static_300m_spawn2m` can expose guarded legacy transition
commands through the `apollo_cyberrt` LaunchPlan. The canonical case uses
`configs/io/examples/phase1_baguang_apollo_followstop_static_control_overlay_paced_compat.yaml`;
the 2m shifted-start diagnostic mitigation uses
`configs/io/examples/phase1_baguang_apollo_followstop_static_spawn2m_control_overlay_low_capture_paced_compat.yaml`.
Both default transition profiles include the full diagnostic control-runtime
overlay because current no-overlay Baguang runs expose the known
`tcmalloc_invalid_free` Apollo control process crash before `/apollo/control`
materializes. The no-overlay profiles remain in `configs/io/examples/` as
reproduction controls for that runtime blocker. The overlay is runtime
compatibility evidence only: it is recorded in the assist ledger as
non-blocking diagnostic context, while `legacy_followstop` remains a blocking
assist for any natural-driving claim.

Dynamic Baguang fixed-scene cases use a separate guarded sidecar transition
profile:
`configs/io/examples/phase1_baguang_apollo_dynamic_sidecar_eager_control_overlay_low_capture_paced_compat.yaml`.
It still inherits the diagnostic overlay/low-capture/pacing stack, but starts
Apollo Control with the Apollo module set instead of waiting for route or
Planning readiness. This exists to test the observed dynamic-scene failure
where the only non-empty Planning message was stale before Control consumed it;
a route-established diagnostic profile exists as a reproduction control, but
the latest route-established sample still started Control too late because
routing success and the only non-empty Planning message arrived together. These
runs still need fixed-scene actor trace, obstacle GT linkage, v-t-gap,
phase1_status, and comparison artifacts before they can count as evaluable
ApolloBackend runs, and they remain Phase 1 diagnostic evidence only. The first
eager-control online sample,
`runs/phase1_online_pairs/lead_decel_70_to_40_20m_eager_control_20260624_010827`,
does produce those artifacts and refreshes to a comparable pair: builtin
succeeds, while Apollo is an evaluable `lane_invasion` behavior failure after
handoff moves to `warn/failure_stage=none`. Treat this as a runtime-boundary
reduction and lateral-semantics blocker, not as Apollo dynamic-following
success.

Use `tools/analyze_phase1_apollo_fixed_scene_dispatch.py` to inspect this
LaunchPlan boundary before online work. A dispatch report with
`guarded_legacy_transition_available` means an operator command exists but has
not produced behavior evidence. A dispatch report with
`runtime_migration_required` is a setup/evidence blocker and must not be counted
as an Apollo backend behavior loss.

Town01 route-only Phase 1 plans are intentionally dispatched as a single
capability step, not the whole historical capability chain. For example,
`town01/lane_keep_097` resolves to:

```bash
python3 tools/run_town01_capability_online_chain.py \
  --step lane_keep:town01_rh_spawn097_goal046 \
  --batch-root-parent <run_dir> \
  --comparison-label-suffix <run_id>
```

`town01/curve217_diagnostic` resolves to
`--step curve_lane_follow:town01_rh_spawn217_goal048`. This keeps the unified
executor aligned with the selected `ScenarioCase`; it still does not prove that
the nested legacy online runner will produce an evaluable Apollo run.
