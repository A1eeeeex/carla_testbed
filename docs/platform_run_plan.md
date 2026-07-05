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

Compile a legacy/experimental suite matrix. The Town01 natural-driving suite is
an Apollo+Autoware diagnostic planning surface, not the current Phase 1 five-P0
ApolloBackend-vs-PlanningControlBackend matrix:

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

The P0 matrix default Apollo platform is
`apollo_cyberrt_town01_behavior_recovery`, which is a Phase 1 diagnostic
profile selected to exercise the current Town01 route-health behavior path.
Use `--apollo-platform apollo_cyberrt` only when intentionally reproducing the
older baseline config. This default is a matrix delivery choice, not an Apollo
natural-driving capability claim.

Attempt the same five P0 pairs online when the local CARLA/Apollo environment
is ready:

```bash
python -m carla_testbed phase1 run-p0-matrix \
  --out runs/phase1_p0_matrix/<id> \
  --timeout-s 180
```

`phase1_p0_matrix_manifest.json` records both orchestration status and
comparison status. Each row includes command execution status, exit codes,
`comparison_status`, `comparison_target_status`, backend Phase 1 statuses, and
backend failure reasons. New rows also include backend primary behavior
blockers, blocker layers, and next-action hints when the run-local
`phase1_status.json` provides them. The top-level summary includes comparable,
partially-evaluable, and invalid pair counts. Use these fields to distinguish
delivery progress from backend behavior outcomes.

The Phase 1 pair and matrix runners can also own a CARLA session explicitly:

```bash
python -m carla_testbed phase1 run-pair \
  --scenario baguang/follow_stop_static_300m_spawn2m \
  --out runs/phase1_online_pairs/<id> \
  --timeout-s 240 \
  --start-carla \
  --carla-town straight_road_for_baguang \
  --carla-extra-args=-RenderOffScreen

python -m carla_testbed phase1 run-p0-matrix \
  --out runs/phase1_p0_matrix/<id> \
  --timeout-s 240 \
  --start-carla \
  --carla-extra-args=-RenderOffScreen
```

For Town01 Apollo compatibility rows, the Apollo backend may record a larger
`effective_runtime_timeout_s` than the user-supplied `--timeout-s`. Current
online evidence requires a `300s` Apollo compatibility floor so the nested
route-health runner can reach its finalized summary instead of being
misclassified as a platform timeout. PlanningControlBackend rows keep the
requested timeout unless their own launch plan declares a minimum.

Phase 1 Apollo postprocess is not fixed-scene-only. Route-only Apollo rows such
as Town01 `lane_keep_097` must refresh the same run-local
`apollo_control_handoff`, `localization_contract`, `control_attribution`,
`control_health`, `apollo_lateral_semantics`, and `apollo_link_health` artifacts
when raw inputs exist. Apollo launch plans now call
`tools/postprocess_phase1_run.py` between `tools/extract_v_t_gap.py` and the
generic `python3 -m carla_testbed analyze` step. `apollo_link_health` is a final
evidence index: run it last, after the Phase 1 and generic per-layer
extraction/analysis steps, so it consumes refreshed reports rather than
reporting stale missing-artifact blockers.

Phase 1 artifact normalization may promote a unique nested legacy
`analysis/route_health/` report set to the declared run root. This exposes
existing route-health evidence for localization/link-health analyzers; it does
not rewrite route-health status, change runtime behavior, or turn Apollo
failures into success.

`tools/postprocess_phase1_run.py` also runs Phase 1 artifact normalization
internally before per-layer analyzers. This matters for legacy Apollo
compatibility commands because launch-plan postprocess runs before the executor
performs its final artifact-surface bookkeeping. The internal normalization
keeps standalone postprocess and launch-plan postprocess consistent.

For `phase1 run-pair`, this writes `carla_session/phase1_carla_session.json`
with `schema_version=phase1_carla_session.v1`, CARLA root, requested town,
readiness diagnostics, and stop status. For `phase1 run-p0-matrix`, the
top-level `carla_session` artifact records `status=per_pair_isolated` and each
pair owns its own `phase1_carla_session.json`. This avoids a legacy Apollo
timeout or nested process contaminating later P0 rows in the same matrix. In
`--dry-run`, the same commands write `status=dry_run_not_started` and do not
start CARLA. The session artifact is startup/environment evidence only; it is
not backend behavior evidence and it does not make Apollo or `carla_builtin`
pass a scenario.

When `phase1 run-pair --start-carla` attaches to an already-running CARLA
server, cleanup leaves that reused server running and records
`carla_session.stop.status=left_running_reused`. This supports repeated online
debug cycles without forcing a CARLA restart after every run. The runner should
only stop CARLA automatically when it launched and owns that server, or when
startup fails before a usable session exists.

The launcher may be invoked from a Python that cannot import the CARLA client
module, for example the project default Python 3.13 while CARLA 0.9.16 provides
only cp310/cp311/cp312 wheels. In that case readiness is probed with the first
working external Python from `CARLA_TESTBED_CARLA_PYTHON`, `CARLA16_PYTHON`, or
common local `carla16` conda paths. The selected probe interpreter is recorded
under `carla_session.diagnostics.client_probe`.

If the session records `status=not_ready` or `status=startup_failed`, treat the
run as a CARLA startup/RPC-readiness blocker. It is not an ApolloBackend or
PlanningControlBackend behavior loss. The runner attempts to stop CARLA on
startup failure and records the result in `carla_session.stop`. For
`phase1 run-pair`, startup failure still materializes backend run directories
with `platform_execution_result.status=blocked_by_carla_startup` and
`analysis/phase1_status.status=invalid`; the pair comparison becomes
`invalid/all_runs_invalid`. For `phase1 run-p0-matrix`, the same failure is
contained inside the affected pair row, so the matrix can continue to later
P0 rows. These are setup/environment artifacts, not backend losses.

CARLA startup readiness does not prove the requested map is loaded. On this
host the initial world observed after `-carla-map=<town>` can still be
`Carla/Maps/Town10HD_Opt`; backend runners must still call/verify
`load_world(<scenario map>)`, and run manifests remain the map-identity
evidence.

For `carla_builtin` fixed-scene runs, the launch plan uses the first executable
Python found in `CARLA_TESTBED_CARLA_PYTHON`, `CARLA16_PYTHON`, or common local
`carla16` conda paths, then falls back to `python3`. Operators should set one
of those variables if their shell `python3` cannot `import carla`. The selected
interpreter is recorded in the launch-plan warnings.

For Apollo Town01 route-only compatibility runs, the launch plan follows the
same interpreter rule. This matters because `tools/run_town01_capability_online_chain.py`
uses CARLA Python APIs during prewarm/load-world checks; running it under a
project interpreter without the CARLA wheel turns the run into
`invalid/no_timeseries` or a timeout setup failure, not an Apollo behavior
loss. The selected interpreter is recorded in `launch_plan.env` as
`CARLA_TESTBED_CARLA_PYTHON` / `CARLA16_PYTHON` and echoed in launch warnings.

For online Baguang pairs, prefer a CARLA session started by the existing
`CarlaLauncher` / `--start-carla` prestart path and keep it alive for the pair.
For the full P0 matrix, the runner starts isolated pair-level CARLA sessions
rather than one shared matrix session because the current legacy Apollo
compatibility path can leave nested processes or world state behind after
timeouts. A raw `CarlaUE4.sh` process may briefly open port `2000` before the
world is actually ready or before `load_world(straight_road_for_baguang)`
succeeds; that state should be treated as environment/startup evidence, not as
a backend behavior result.

If a real runtime command is attempted and safe postprocess returns without
writing `analysis/phase1_status/phase1_status.json`, `ScenarioRunExecutor`
materializes a fallback Phase 1 status using the existing classifier. This
keeps failed/timeout runs inspectable and prevents missing postprocess output
from being mistaken for a backend behavior loss. The fallback does not run in
dry-run mode and does not overwrite an existing status artifact.

The runtime adapter starts backend commands in their own process group and, as
a second safety net, terminates residual processes whose command line references
the current run directory. This is needed for legacy wrappers that spawn a new
session through `conda run` or nested CARLA/Apollo helpers. Residual cleanup is
lifecycle evidence only; it must not be interpreted as backend behavior success.
When residuals are found, the adapter writes
`execution/residual_process_cleanup.json` and adds a cleanup warning such as
`terminated_residual_processes_for_run_dir:<count>`.

The builtin PlanningControlBackend runner uses retry-backed CARLA world loading
for both route-only and fixed-scene runs. Attempts are recorded in
`artifacts/carla_load_world_attempts.jsonl`. A transient `load_world` timeout is
therefore observable and retryable, while a final failure still leaves the run
`invalid/no_timeseries`.

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

If that nested legacy runner writes exactly one `timeseries.csv` or
`timeseries.jsonl` below the declared Phase 1 run directory, the executor
normalizes the artifact surface by copying it to the declared run root and
writing `analysis/phase1_artifact_normalization/phase1_artifact_normalization_report.json`.
The same uniqueness rule applies to `events.jsonl`,
`config.resolved.yaml`, and known control trace surfaces such as
`artifacts/control_apply_trace.jsonl`. For route-only scenarios, the executor
can write `v_t_gap.status=not_applicable` and Phase 1 artifact-completeness
reports after normalization. These reports are evidence routing only: timeout
remains timeout, lane invasion remains lane invasion, and Apollo behavior
success is not inferred from artifact normalization.

Historical online validation of this path is
`runs/phase1_p0_matrix/phase1_p0_auto_artifacts_20260625_153515`: all five P0
pairs were comparable under the older artifact-normalization surface. The newer
Phase 1 behavior-default matrix supersedes it for current status claims and
must be attached with exact run artifacts before external review treats it as
independently verified.
