# Run Artifacts

This document defines the minimum self-contained output surface for a CARLA
testbed run. A run directory should answer four questions: what was requested,
what config was resolved, what happened, and what evidence can be compared.

## Output Directory Structure

The standard layout is:

```text
runs/<run_id>/
  manifest.json
  config.resolved.yaml
  summary.json
  events.jsonl
  timeseries.csv
  logs/
```

`timeseries.jsonl` is also supported by `RunArtifactStore` for future backends.
The legacy harness still writes CSV-oriented frame data.

## `manifest.json`

`manifest.json` records static run context and identity. Minimum fields:

- `run_id`
- `start_time_wall_s`
- `end_time_wall_s`, usually added at close
- `config_path`
- `git_sha`, when available
- CARLA `host`, `port`, and `town`, when known
- `carla_world`, when runtime CARLA was available. This records
  `configured_town`, the observed `loaded_map_name`, short-name match status,
  and spawn-point count so evidence can distinguish requested map identity from
  the world actually loaded by CARLA.
- `scenario_name`
- `backend_name`
- non-critical metadata such as smoke mode or runtime notes

Use the manifest to identify what the run claimed to execute. Do not use it as
a replacement for per-frame evidence.

## `config.resolved.yaml`

`config.resolved.yaml` is the resolved configuration snapshot for the run. It
should reflect built-in defaults, main config, local override, and CLI
overrides after merging.

Do not commit resolved configs that contain private host paths or secrets.
Tracked examples should use environment placeholders or local override
templates instead.

## `summary.json`

`summary.json` is the final result surface. Minimum fields:

- `success`
- `exit_reason`
- `frames`
- `sim_duration_s`
- `wall_duration_s`
- `cleanup_errors_count`
- `metrics`

The `metrics` block is intentionally small and comparable:

- `frames`
- `sim_duration_s`
- `wall_duration_s`
- `collision_count`
- `lane_invasion_count`, when available
- `avg_speed_mps`
- `max_speed_mps`
- `min_lead_distance_m`, when available
- `control_frames`
- `exit_reason`

If a value cannot be computed, use `null` or an explicit unknown value. Do not
invent metrics to make a run look healthier.

## `events.jsonl`

`events.jsonl` stores discrete run events, one JSON object per line. Typical
events include:

- `run_start`
- `smoke_start`
- `collision`
- `lane_invasion`
- `backend_warning`
- `failure`
- `run_end`

Events should be useful for debugging without scanning a full timeseries.

## `timeseries.csv` / `timeseries.jsonl`

Timeseries files store frame-level evidence. The legacy harness CSV path may
include:

- frame id and simulator time
- ego speed
- lead distance when available
- raw control command
- clamped/applied control
- collision and lane invasion counters
- controller debug fields

Future backends may prefer `timeseries.jsonl` for structured per-frame records.
Both forms should stay run-local.

## `logs/`

`logs/` is optional. It may contain captured simulator, backend, bridge, or
adapter logs. For no-runtime smoke runs it may be empty.

## Town01 Truth-Input Diagnostics

Town01 Apollo truth-input runs that are used for natural-driving or curve
diagnosis need richer evidence than the base artifact surface. Expected
run-local diagnostics include:

- `analysis/route_health/route_health.json`
- `analysis/route_health/route_health.csv`
- `analysis/route_health/curve_segments.csv`
- `analysis/route_health/route_health_summary.md`
- `analysis/apollo_channel_health/apollo_channel_health_report.json`
- `analysis/control_health/control_health_report.json`
- `analysis/failure_timeline/failure_timeline_report.json`
- `analysis/route_start_alignment/route_start_alignment_report.json`
- `analysis/artifact_completeness/artifact_completeness_report.json`
- `analysis/traffic_light/traffic_light_contract_report.json` for traffic-light scenarios
- `analysis/traffic_light/traffic_light_behavior_report.json` for traffic-light scenarios

`natural_driving_report.json` must treat a missing
`artifact_completeness_report.json` as `insufficient_data`. This keeps the
evidence chain persisted in the run directory instead of relying on an in-memory
check or visual observation.

`artifact_completeness_report.json` also treats missing or mismatched
`manifest.json.carla_world` identity as `insufficient_data`. A run may request
`Town01` in config, but capability evidence should separately record the CARLA
map that was actually loaded.

## Current Implementation

`carla_testbed.record.artifact_store.RunArtifactStore` owns the standard file
paths and writers:

- manifest writes and updates
- resolved config snapshot
- summary writes
- event writer
- timeseries writer

The current runner is partially migrated. It writes the standard artifact
surface for new smoke/config paths, while some legacy and Town01 operational
tools still have richer historical artifact families. Preserve those until the
new artifact surface can represent the same evidence.

## Inspecting Runs

Use:

```bash
python -m carla_testbed inspect-run <run_dir>
```

This reads `manifest.json` and `summary.json` and prints a compact result. It
does not replace detailed artifact review for Apollo/Town01 capability
promotion.
