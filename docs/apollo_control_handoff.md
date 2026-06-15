# Apollo Control Handoff Evidence

This workflow checks whether Apollo Control output actually travels through the runtime chain before any natural-driving or A/B claim uses control behavior as evidence.

## Evidence Layers

The report file is `apollo_control_handoff_report.json` with schema version `apollo_control_handoff.v1`.

The analyzer separates seven layers:

1. `process_health`: Control process expected, started, stayed alive, and did not crash. Known crash signatures include tcmalloc invalid free, segfault, abort, protobuf descriptor issues, DAG load failures, and module exits.
2. `input_readiness`: Control inputs are available: planning, localization, chassis, and control-pad/enable evidence when that requirement is known.
3. `control_channel`: `/apollo/control` has messages, timestamp evidence when available, and raw command fields.
4. `bridge_receive`: the bridge receives Apollo control messages.
5. `raw_decode`: the bridge can decode raw throttle, brake, and steering fields.
6. `mapping_and_apply`: raw commands are mapped and applied to CARLA, with `apply_control_count > 0`.
7. `vehicle_response`: after applied control, the vehicle shows speed, route progress, or yaw response evidence.

Failure stages are prioritized upstream first. For example, if Control crashes, downstream bridge/apply layers are not guessed.
When `process_health` reports a crash before usable control output, downstream
control-health and link-health reports should surface
`control_process_crash_before_control_output` before investigating actuation
oscillation or PID/calibration hypotheses.

## Gate Usage

Natural-driving hard pass in `natural_driving_report.json` requires:

- `verdict` is `pass` or `warn`.
- `failure_stage` is `none`.
- `control_channel.message_count > 0`.
- `bridge_receive.control_rx_count > 0`.
- `mapping_and_apply.apply_control_count > 0`.

If the report is missing or insufficient, the run can still be debugged, but it cannot support a hard natural-driving pass in `natural_driving_report.json`.

A/B analysis records handoff status and failure stage. A candidate with a failed control handoff cannot be reported as `candidate_positive`.

For no-interference Apollo truth-input evidence, the control handoff report is
read together with localization, Apollo reference-line, control-health, and
link-health reports. The operator should run:

```bash
RUN=runs/<run_id>
python tools/analyze_apollo_control_handoff.py --run-dir "$RUN"
python tools/analyze_apollo_link_health.py --run-dir "$RUN"
```

`natural_driving_report.json` can only use this layer as non-blocking evidence
when the report is `pass` or `warn`, `failure_stage=none`, Apollo `/control`
messages are present, bridge receive/decode counters are positive, and CARLA
apply evidence is positive. A successful handoff does not prove behavior
quality; it only proves the source command reached the bridge and CARLA apply
surface.

For a no-interference natural-driving claim, follow the full checklist in
`docs/apollo_town01_truth_natural_driving.md`; this handoff report is one
required artifact, not the final gate.

## Boundaries

This report does not change `steer_scale`, does not enable physical mapping, and does not tune Apollo Control. It only records evidence across source control semantics, bridge decoding, mapped/applied controls, and CARLA vehicle response.

`control_health_report.json` decomposes oscillation into Apollo raw command, bridge mapped command, CARLA applied command, vehicle response, and bridge apply cadence. Applied actuation oscillation is only claim-grade control evidence after `localization_contract_report.json` and `apollo_reference_line_contract_report.json` are `pass` or `warn` with no blocking reasons. If those upstream contracts are missing or blocking, applied oscillation remains secondary evidence and should not be hidden with smoothing or PID changes.

For Apollo CyberRT truth-input runs, `artifacts/control_apply_trace.jsonl` is
the preferred row-level control-chain evidence. It records Apollo raw command,
bridge mapped command, CARLA applied command, vehicle response, route progress,
latency, actuator mapping mode, calibration profile, steering sign/scale, and
apply-cadence diagnostics in the same row. This lets the handoff and
control-health analyzers attribute breakpoints without smoothing away the raw
source command.
When available, each row also records Apollo control header sequence and
timestamp metadata under `apollo_control`. `control_health_report.json` reports
both row-level throttle/brake switch counts and switch counts compressed by
unique `header_sequence_num`; this is how operators tell repeated application
of one command apart from genuinely oscillating Apollo `/control` output.
If `control_apply_trace.jsonl` is the primary raw/mapped/applied trace but does
not yet contain Apollo `simple_lon` debug context, `control_health_report.json`
may read `control_decode_debug.jsonl` as an auxiliary source for longitudinal
source-control semantics only. That auxiliary source must not replace the
primary raw -> mapped -> applied evidence chain.
`control_trajectory_consume_debug.jsonl` and the live variant
`control_trajectory_consume_debug_live.jsonl` are both accepted as planning
handoff correlation evidence. They let the report distinguish command switching
caused by planning sequence/trajectory updates from switching that happens
inside the same consumed trajectory.
`control_health_report.json` also exposes the same evidence through
`control_semantics_primary_factor`, `control_semantics_suspected_factors`, and
`control_semantics_evidence` so `apollo_link_health_report.json` can surface
the highest-value control diagnostic target without reimplementing the nested
debug parsing. These fields are diagnostic summaries only; they do not override
localization, HDMap projection, reference-line, control-handoff, or
natural-driving gates.
The same report also writes
`metrics.control_oscillation_diagnosis`, a compact roll-up for operator triage.
It flags whether raw Apollo command switching is present, whether switching
occurs on the same consumed trajectory, whether GT localization/chassis is
being oversampled by the Apollo control loop, whether legacy mapping is only
smoke/debug evidence, and which next debug target should be inspected first.
This roll-up is still attribution evidence, not a capability verdict.
For row-level navigation, inspect
`metrics.control_decode_debug.planning_trajectory_correlation.transition_window_summary`.
It records the ratio of throttle/brake transitions that happen within the same
Planning sequence versus transitions aligned with a Planning sequence update,
plus small sample windows with row indices, states, planning age, trajectory
type, speed reference, acceleration command, and path-remain values.
Trace-file presence alone is not control evidence. Rows whose raw, mapped, and
applied command fields are all null are treated as no-command placeholders; they
can document that the bridge loop ran, but they cannot clear raw/mapped/applied
control gates or support a no-interference claim.

When P0 `timeseries.csv` lacks raw or mapped command fields, and
`control_apply_trace.jsonl` is not available, the analyzers may use
`artifacts/control_decode_debug.jsonl` or `artifacts/bridge_control_decode.jsonl`
as fallback evidence for the Apollo raw command and bridge mapped command
layers. That fallback can clear the handoff
`mapping_and_apply.mapped_control` gap, but it does not by itself prove
mapped-to-applied correctness; CARLA applied fields,
`control_apply_trace.jsonl`, direct apply rows, or vehicle response fields are
still required for the applied and response layers.

Bridge apply cadence and same-frame drop are checked before actuation tuning. If `apply_hz` is not met, or same-frame drops are not explained by CARLA synchronous world ticks, fix the tick/apply scheduling evidence first. Legacy `actuator_mapping_mode` remains smoke/debug evidence; claim-grade control mapping requires physical/calibrated mapping or an explicit calibration profile, and the report must preserve raw -> mapped -> applied command fields.

Apollo Docker deferred Control startup can be gated on `planning_ready`. The
startup is asynchronous by default (`deferred_control_start_async=true`) so the
blocking Docker/CyberRT module launch and survival probe do not stall CARLA
world-tick cadence. Operators should verify
`artifacts/carla_tick_health_summary.json` and
`artifacts/carla_tick_health.jsonl`: a healthy async startup should leave
`CyberRTBackend.on_sim_tick` as a sub-second hook, with the actual Control
startup sequence recorded in `artifacts/apollo_backend_startup_trace.jsonl` and
`artifacts/apollo_control_deferred_*.log`. This only proves handoff scheduling
is observable; it does not prove Apollo Control behavior is correct.

If the dominant issue appears to be source control semantics, this still does not by itself prove an Apollo algorithm limitation. Check route/reference-line, matched point, target point, localization contract, and lateral semantics reports before making that claim.

## Commands

```bash
python tools/analyze_apollo_control_handoff.py \
  --run-dir runs/<run_id> \
  --out runs/<run_id>/analysis/apollo_control_handoff
```

The natural-driving postprocess wrapper also generates this report offline when artifacts are available:

```bash
python tools/postprocess_town01_natural_driving.py --suite-root runs/<suite_id>
```
