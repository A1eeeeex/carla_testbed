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

## Boundaries

This report does not change `steer_scale`, does not enable physical mapping, and does not tune Apollo Control. It only records evidence across source control semantics, bridge decoding, mapped/applied controls, and CARLA vehicle response.

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
